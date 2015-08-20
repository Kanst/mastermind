/*
   Copyright (c) YANDEX LLC, 2015. All rights reserved.
   This file is part of Mastermind.

   Mastermind is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3.0 of the License, or (at your option) any later version.

   Mastermind is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with Mastermind.
*/

#include "FS.h"
#include "Metrics.h"
#include "Round.h"
#include "WorkerApplication.h"

#include <errno.h>
#include <sys/epoll.h>
#include <fcntl.h>

using namespace ioremap;

namespace {

class CurlGuard
{
public:
    CurlGuard(CURLM* & multi, int & epollfd)
        :
        m_multi(multi),
        m_epollfd(epollfd)
    {}

    ~CurlGuard()
    {
        if (m_multi != nullptr) {
            curl_multi_cleanup(m_multi);
            m_multi = nullptr;
        }

        if (m_epollfd >= 0) {
            close(m_epollfd);
            m_epollfd = -1;
        }
    }

private:
    CURLM* & m_multi;
    int & m_epollfd;
};

void empty_function(void *)
{}

} // unnamed namespace

Round::ClockStat::ClockStat()
{
    std::memset(this, 0, sizeof(*this));
}

Round::ClockStat & Round::ClockStat::operator = (const ClockStat & other)
{
    std::memcpy(this, &other, sizeof(*this));
    return *this;
}

class Round::GroupMetadataHandle
{
public:
    GroupMetadataHandle(Round & round, Group & group, elliptics::session & session)
        :
        m_round(round),
        m_group(group),
        m_session(session.clone())
    {}

    Round & get_round()
    { return m_round; }

    Group & get_group()
    { return m_group; }

    elliptics::session & get_session()
    { return m_session; }

    void result(const elliptics::read_result_entry & entry)
    {
        elliptics::data_pointer file = entry.file();
        m_group.save_metadata((const char *) file.data(), file.size());
    }

    void final(const elliptics::error_info & error)
    {
        if (error) {
            std::ostringstream ostr;
            ostr << "Metadata download failed: " << error.message();
            m_group.set_status_text(ostr.str());
        }
        m_round.handle_group_download_completed();
        delete this;
    }

private:
    Round & m_round;
    Group & m_group;
    elliptics::session m_session;
};

Round::Round(Collector & collector)
    :
    m_collector(collector),
    m_storage(collector.get_storage()),
    m_session(collector.get_discovery().get_session().clone()),
    m_type(REGULAR),
    m_epollfd(-1),
    m_curl_handle(nullptr),
    m_timeout_ms(0)
{
    clock_start(m_clock.total);
    m_queue = dispatch_queue_create("round", DISPATCH_QUEUE_CONCURRENT);
}

Round::Round(Collector & collector, std::shared_ptr<on_force_update> handler)
    :
    m_collector(collector),
    m_storage(collector.get_storage()),
    m_session(collector.get_discovery().get_session().clone()),
    m_type(FORCED_FULL),
    m_epollfd(-1),
    m_curl_handle(nullptr),
    m_timeout_ms(0)
{
    clock_start(m_clock.total);
    m_queue = dispatch_queue_create("round", DISPATCH_QUEUE_CONCURRENT);
    m_on_force_handler = handler;
}

Round::~Round()
{
    dispatch_release(m_queue);
}

WorkerApplication & Round::get_app()
{
    return m_collector.get_app();
}

void Round::start()
{
    BH_LOG(get_app().get_logger(), DNET_LOG_INFO,
            "Starting %s discovery with %lu nodes",
            (m_type == REGULAR) ? "regular" : (m_type == FORCED_FULL) ? "forced full" : "forced partial",
            m_storage.get_nodes().size());

    dispatch_async_f(m_queue, this, &Round::step2_curl_download);
}

void Round::step2_curl_download(void *arg)
{
    Round & self = *static_cast<Round*>(arg);

    self.perform_download();

    clock_start(self.m_clock.finish_monitor_stats);
    dispatch_barrier_async_f(self.m_queue, &self, &Round::step3_prepare_metadata_download);
}

void Round::step3_prepare_metadata_download(void *arg)
{
    Round & self = *static_cast<Round*>(arg);

    clock_stop(self.m_clock.finish_monitor_stats);

    self.m_storage.update_group_structure();
    std::map<int, Group> & groups = self.m_storage.get_groups();

    BH_LOG(self.get_app().get_logger(), DNET_LOG_INFO, "Scheduling metadata download for %lu groups", groups.size());
    clock_start(self.m_clock.metadata_download);

    self.m_nr_groups = groups.size();
    for (auto it = groups.begin(); it != groups.end(); ++it) {
        Group & group = it->second;
        GroupMetadataHandle *handle = new GroupMetadataHandle(self, group, self.m_session);
        dispatch_async_f(self.m_queue, handle, &Round::request_group_metadata);
    }
}

void Round::step4_perform_update(void *arg)
{
    Round & self = *static_cast<Round*>(arg);

    Stopwatch watch(self.m_clock.storage_update);
    self.m_storage.update();
    watch.stop();

    self.m_collector.finalize_round(&self);
}

void Round::request_group_metadata(void *arg)
{
    GroupMetadataHandle *handle = static_cast<GroupMetadataHandle*>(arg);

    std::vector<int> group_id(1, handle->get_group().get_id());
    static const elliptics::key key("symmetric_groups");

    elliptics::session & session = handle->get_session();
    session.set_namespace("metabalancer");
    session.set_groups(group_id);

    BH_LOG(handle->get_round().get_app().get_logger(), DNET_LOG_DEBUG,
            "Scheduling metadata download for group %d", group_id[0]);

    elliptics::async_read_result res = session.read_data(key, group_id, 0, 0);
    res.connect(std::bind(&GroupMetadataHandle::result, handle, std::placeholders::_1),
            std::bind(&GroupMetadataHandle::final, handle, std::placeholders::_1));
}

int Round::perform_download()
{
    Stopwatch watch(m_clock.perform_download);

    CurlGuard guard(m_curl_handle, m_epollfd);

    int res = 0;

    m_curl_handle = curl_multi_init();
    if (!m_curl_handle) {
        BH_LOG(get_app().get_logger(), DNET_LOG_ERROR, "curl_multi_init() failed");
        return -1;
    }

    curl_multi_setopt(m_curl_handle, CURLMOPT_SOCKETFUNCTION, handle_socket);
    curl_multi_setopt(m_curl_handle, CURLMOPT_SOCKETDATA, (void *) this);
    curl_multi_setopt(m_curl_handle, CURLMOPT_TIMERFUNCTION, handle_timer);
    curl_multi_setopt(m_curl_handle, CURLMOPT_TIMERDATA, (void *) this);

    m_epollfd = epoll_create(1);
    if (m_epollfd < 0) {
        int err = errno;
        BH_LOG(get_app().get_logger(), DNET_LOG_ERROR, "epoll_create() failed: %s", strerror(err));
        return -1;
    }

    std::map<std::string, Node> & nodes = m_storage.get_nodes();

    for (auto it = nodes.begin(); it != nodes.end(); ++it) {
        Node & node = it->second;

        BH_LOG(get_app().get_logger(), DNET_LOG_INFO, "Scheduling stat download for node %s",
                node.get_key().c_str());

        CURL *easy = create_easy_handle(&node);
        if (easy == nullptr) {
            BH_LOG(get_app().get_logger(), DNET_LOG_ERROR,
                    "Cannot create easy handle to download node stat");
            return -1;
        }
        curl_multi_add_handle(m_curl_handle, easy);
    }

    int running_handles = 0;

    // kickstart download
    curl_multi_socket_action(m_curl_handle, CURL_SOCKET_TIMEOUT, 0, &running_handles);

    while (running_handles) {
        struct epoll_event event;
        int rc = epoll_wait(m_epollfd, &event, 1, 100);

        if (rc < 0) {
            if (errno == EINTR)
                continue;

            int err = errno;
            BH_LOG(get_app().get_logger(), DNET_LOG_ERROR, "epoll_wait() failed: %s", strerror(err));
            return -1;
        }

        if (rc == 0)
            curl_multi_socket_action(m_curl_handle, CURL_SOCKET_TIMEOUT, 0, &running_handles);
        else
            curl_multi_socket_action(m_curl_handle, event.data.fd, 0, &running_handles);

        struct CURLMsg *msg;
        do {
            int msgq = 0;
            msg = curl_multi_info_read(m_curl_handle, &msgq);

            if (msg && (msg->msg == CURLMSG_DONE)) {
                CURL *easy = msg->easy_handle;

                Node *node = nullptr;
                CURLcode cc = curl_easy_getinfo(easy, CURLINFO_PRIVATE, &node);
                if (cc != CURLE_OK) {
                    BH_LOG(get_app().get_logger(), DNET_LOG_ERROR, "curl_easy_getinfo() failed");
                    return -1;
                }

                if (msg->data.result == CURLE_OK) {
                    BH_LOG(get_app().get_logger(), DNET_LOG_INFO,
                            "Node %s stat download completed", node->get_key().c_str());
                    dispatch_async_f(m_queue, node, &Node::parse_stats);
                } else {
                    BH_LOG(get_app().get_logger(), DNET_LOG_ERROR, "Node %s stats download failed, "
                            "result: %d", node->get_key().c_str(), msg->data.result);
                    node->drop_download_data();
                }

                curl_multi_remove_handle(m_curl_handle, easy);
                curl_easy_cleanup(easy);

            }
        } while (msg);
    }

    return res;
}

CURL *Round::create_easy_handle(Node *node)
{
    CURL *easy = curl_easy_init();
    if (easy == nullptr)
        return nullptr;

    char buf[128];
    sprintf(buf, "http://%s:%lu/?categories=80", node->get_host().c_str(), get_app().get_config().monitor_port);

    curl_easy_setopt(easy, CURLOPT_URL, buf);
    curl_easy_setopt(easy, CURLOPT_PRIVATE, node);
    curl_easy_setopt(easy, CURLOPT_ACCEPT_ENCODING, "deflate");
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, &write_func);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, node);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT, get_app().get_config().wait_timeout);

    return easy;
}

int Round::handle_socket(CURL *easy, curl_socket_t fd,
        int action, void *userp, void *socketp)
{
    Round *self = (Round *) userp;
    if (self == nullptr)
        return -1;

    struct epoll_event event;
    event.events = 0;
    event.data.fd = fd;

    if (action == CURL_POLL_REMOVE) {
        int rc = epoll_ctl(self->m_epollfd, EPOLL_CTL_DEL, fd, &event);
        if (rc != 0 && errno != EBADF) {
            int err = errno;
            BH_LOG(self->get_app().get_logger(), DNET_LOG_WARNING, "CURL_POLL_REMOVE: %s", strerror(err));
        }
        return 0;
    }

    event.events = (action == CURL_POLL_INOUT) ? (EPOLLIN | EPOLLOUT) :
                   (action == CURL_POLL_IN) ? EPOLLIN :
                   (action == CURL_POLL_OUT) ? EPOLLOUT : 0;
    if (!event.events)
        return 0;

    int rc = epoll_ctl(self->m_epollfd, EPOLL_CTL_ADD, fd, &event);
    if (rc < 0) {
        if (errno == EEXIST)
            rc = epoll_ctl(self->m_epollfd, EPOLL_CTL_MOD, fd, &event);
        if (rc < 0) {
            int err = errno;
            BH_LOG(self->get_app().get_logger(), DNET_LOG_WARNING, "EPOLL_CTL_MOD: %s", strerror(err));
            return -1;
        }
    }

    return 0;
}

int Round::handle_timer(CURLM *multi, long timeout_ms, void *userp)
{
    Round *self = (Round *) userp;
    if (self == nullptr)
        return -1;

    self->m_timeout_ms = timeout_ms;

    return 0;
}

size_t Round::write_func(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    if (userdata == nullptr)
        return 0;

    Node *node = (Node *) userdata;
    node->add_download_data(ptr, size * nmemb);
    return size * nmemb;
}

void Round::handle_group_download_completed()
{
    if (! --m_nr_groups) {
        BH_LOG(get_app().get_logger(), DNET_LOG_INFO, "Group metadata download completed");
        clock_stop(m_clock.metadata_download);

        dispatch_async_f(m_queue, this, &Round::step4_perform_update);
    }
}