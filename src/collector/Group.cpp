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

#include "Backend.h"
#include "Config.h"
#include "Couple.h"
#include "Filter.h"
#include "FS.h"
#include "Group.h"
#include "Metrics.h"
#include "Node.h"
#include "Storage.h"
#include "WorkerApplication.h"

#include <msgpack.hpp>

#include <algorithm>
#include <cstring>
#include <sstream>

namespace {

bool parse_couple(msgpack::object & obj, std::vector<int> & couple)
{
    if (obj.type != msgpack::type::ARRAY)
        return false;

    for (uint32_t i = 0; i < obj.via.array.size; ++i) {
        msgpack::object & gr_obj = obj.via.array.ptr[i];
        if (gr_obj.type != msgpack::type::POSITIVE_INTEGER)
            return false;
        couple.push_back(int(gr_obj.via.u64));
    }

    std::sort(couple.begin(), couple.end());

    return true;
}

} // unnamed namespace

Group::Group(Storage & storage, int id)
    :
    m_storage(storage),
    m_id(id),
    m_couple(nullptr),
    m_clean(true),
    m_status(INIT),
    m_metadata_process_start(0),
    m_metadata_process_time(0),
    m_frozen(false),
    m_version(0),
    m_namespace(nullptr)
{
    m_service.migrating = false;
}

Group::Group(Storage & storage)
    :
    m_storage(storage),
    m_id(0),
    m_couple(nullptr),
    m_clean(true),
    m_status(INIT),
    m_metadata_process_start(0),
    m_metadata_process_time(0),
    m_frozen(false),
    m_version(0),
    m_namespace(nullptr)
{
    m_service.migrating = false;
}

void Group::clone_from(const Group & other)
{
    m_id = other.m_id;
    merge(other);
}

bool Group::has_backend(Backend & backend) const
{
    auto it = m_backends.find(&backend);
    return (it != m_backends.end());
}

void Group::add_backend(Backend & backend)
{
    m_backends.insert(&backend);
}

bool Group::full() const
{
    for (const Backend *backend : m_backends) {
        if (!backend->full())
            return false;
    }
    return true;
}

uint64_t Group::get_total_space() const
{
    uint64_t res = 0;

    for (const Backend *backend : m_backends)
        res += backend->get_total_space();
    return res;
}

void Group::save_metadata(const char *metadata, size_t size)
{
    if (m_clean && !m_metadata.empty() && m_metadata.size() == size &&
            !std::memcmp(&m_metadata[0], metadata, size))
        return;

    m_metadata.assign(metadata, metadata + size);
    m_clean = false;
}

void Group::process_metadata()
{
    if (m_clean)
        return;

    clock_start(m_metadata_process_start);
    Stopwatch watch(m_metadata_process_time);

    m_clean = true;

    std::ostringstream ostr;

    m_status_text.clear();

    msgpack::unpacked result;
    msgpack::object obj;

    try {
        msgpack::unpack(&result, &m_metadata[0], m_metadata.size());
        obj = result.get();
    } catch (std::exception & e) {
        ostr << "msgpack could not parse group metadata: " << e.what();
    } catch (...) {
        ostr << "msgpack could not parse group metadta with unknown reason";
    }

    if (!ostr.str().empty()) {
        m_status_text = ostr.str();
        m_status = BAD;
        return;
    }

    int version = 0;
    std::vector<int> couple;
    std::string ns;
    bool frozen = false;
    bool service_migrating = false;
    std::string service_job_id;

    if (obj.type == msgpack::type::MAP) {
        for (uint32_t i = 0; i < obj.via.map.size; ++i) {
            msgpack::object_kv & kv = obj.via.map.ptr[i];
            if (kv.key.type != msgpack::type::RAW)
                continue;

            uint32_t size = kv.key.via.raw.size;
            const char *ptr = kv.key.via.raw.ptr;

            if (size == 7 && !std::strncmp(ptr, "version", 7)) {
                if (kv.val.type == msgpack::type::POSITIVE_INTEGER) {
                    version = int(kv.val.via.u64);
                } else {
                    ostr << "Invalid 'version' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 6 && !std::strncmp(ptr, "couple", 6)) {
                if (!parse_couple(kv.val, couple)) {
                    ostr << "Couldn't parse 'couple'" << std::endl;
                    break;
                }
            }
            else if (size == 9 && !std::strncmp(ptr, "namespace", 9)) {
                if (kv.val.type == msgpack::type::RAW) {
                    ns.assign(kv.val.via.raw.ptr, kv.val.via.raw.size);
                } else {
                    ostr << "Invalid 'namespace' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 6 && !std::strncmp(ptr, "frozen", 6)) {
                if (kv.val.type == msgpack::type::BOOLEAN) {
                    frozen = kv.val.via.boolean;
                } else {
                    ostr << "Invalid 'frozen' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 7 && !std::strncmp(ptr, "service", 7)) {
                if (kv.val.type == msgpack::type::MAP) {
                    bool ok = true;

                    for (uint32_t j = 0; j < kv.val.via.map.size; ++j) {
                        msgpack::object_kv & srv_kv = kv.val.via.map.ptr[j];
                        if (srv_kv.key.type != msgpack::type::RAW)
                            continue;

                        uint32_t srv_size = srv_kv.key.via.raw.size;
                        const char *srv_ptr = srv_kv.key.via.raw.ptr;

                        if (srv_size == 6 && !std::strncmp(srv_ptr, "status", 6)) {
                            // XXX
                            if (srv_kv.val.type == msgpack::type::RAW) {
                                uint32_t st_size = srv_kv.val.via.raw.size;
                                const char *st_ptr = srv_kv.val.via.raw.ptr;
                                if (st_size == 9 && !std::strncmp(st_ptr, "MIGRATING", 9))
                                    service_migrating = true;
                            }
                        }
                        else if (srv_size == 6 && !std::strncmp(srv_ptr, "job_id", 6)) {
                            if (srv_kv.val.type == msgpack::type::RAW) {
                                service_job_id.assign(
                                        srv_kv.val.via.raw.ptr, srv_kv.val.via.raw.size);
                            } else {
                                ostr << "Invalid 'job_id' value type " << srv_kv.val.type;
                                ok = false;
                                break;
                            }
                        }
                    }
                    if (!ok)
                        break;
                } else {
                    ostr << "Invalid 'service' value type " << kv.val.type;
                    break;
                }
            }
        }
    }
    else if (obj.type == msgpack::type::ARRAY) {
        version = 1;
        ns = "default";
        if (!parse_couple(obj, couple))
            ostr << "Couldn't parse couple (format of version 1)";
    }

    if (!ostr.str().empty()) {
        m_status_text = ostr.str();
        m_status = BAD;
        return;
    }

    m_version = version;
    m_frozen = frozen;
    m_service.migrating = service_migrating;
    m_service.job_id = service_job_id;

    if (m_namespace == nullptr) {
        m_namespace = &m_storage.get_namespace(ns);
    } else if (m_namespace->get_name() != ns) {
        m_status = BAD;
        ostr << "Group moved to another namespace: '"
             << m_namespace->get_name() << "' -> '"
             << ns << '\'';
        m_status_text = ostr.str();
        return;
    }

    if (m_couple != nullptr) {
        if (!m_couple->check(couple)) {
            std::vector<int> couple_groups;
            m_couple->get_group_ids(couple_groups);

            ostr << "Couple in group metadata [ ";
            for (size_t i = 0; i < couple.size(); ++i)
                ostr << couple[i] << ' ';
            ostr << "] doesn't match to existing one [ ";
            for (size_t i = 0; i < couple_groups.size(); ++i)
                ostr << couple_groups[i] << ' ';
            ostr << ']';

            m_status_text = ostr.str();
            m_status = BAD;

            return;
        }
    } else {
        m_storage.create_couple(couple, this);
    }

    if (m_backends.empty()) {
        m_status = INIT;
        m_status_text = "No node backends";
    } else if (m_backends.size() > 1 && m_storage.get_app().get_config().forbidden_dht_groups) {
        m_status = BROKEN;

        ostr << "DHT groups are forbidden but the group has " << m_backends.size() << " backends";
        m_status_text = ostr.str();
    } else {
        bool have_bad = false;
        bool have_ro = false;
        bool have_other = false;

        for (Backend *backend : m_backends) {
            Backend::Status b_status = backend->get_status();
            if (b_status == Backend::BAD) {
                have_bad = true;
                break;
            } else if (b_status == Backend::RO) {
                have_ro = true;
            } else if (b_status != Backend::OK) {
                have_other = true;
            }
        }

        if (have_bad) {
            m_status = BROKEN;
            m_status_text = "Some of backends are in state BROKEN";
        } else if (have_ro) {
            if (m_service.migrating) {
                m_status = MIGRATING;
                ostr << "Group is migrating, job id is '" << m_service.job_id << '\'';
                m_status_text = ostr.str();
                // TODO: check whether the job was initiated
            } else {
                m_status = RO;
                m_status_text = "Group is read-only because it has read-only backends";
            }
        } else if (have_other) {
            m_status = BAD;
            m_status_text = "Group is in state BAD because some of "
                "backends are not in state OK";
        } else {
            m_status = COUPLED;
            m_status_text = "Group is OK";
        }
    }
}

bool Group::check_metadata_equals(const Group & other) const
{
    if (m_status == INIT || other.m_status == INIT)
        return true;

    return (m_frozen == other.m_frozen &&
            m_couple == other.m_couple &&
            m_namespace == other.m_namespace);
}

void Group::set_status_text(const std::string & status_text)
{
    m_status_text = status_text;
}

void Group::get_status_text(std::string & status_text) const
{
    status_text = m_status_text;
}

void Group::get_job_id(std::string & job_id) const
{
    job_id = m_service.job_id;
}

void Group::merge(const Group & other)
{
    if (m_metadata_process_start >= other.m_metadata_process_start)
        return;

    m_clean = other.m_clean;
    m_metadata = other.m_metadata;
    m_status_text = other.m_status_text;
    m_status = other.m_status;

    m_metadata_process_start = other.m_metadata_process_start;
    m_metadata_process_time = other.m_metadata_process_time;

    m_frozen = other.m_frozen;
    m_version = other.m_version;
    m_service.migrating = other.m_service.migrating;
    m_service.job_id = other.m_service.job_id;

    if (other.m_couple != nullptr) {
        if (m_couple == nullptr) {
            Couple *couple = nullptr;
            if (m_storage.get_couple(other.m_couple->get_key(), couple)) {
                m_couple = couple;
            } else {
                std::vector<int> group_ids;
                other.m_couple->get_group_ids(group_ids);
                m_storage.create_couple(group_ids, this);
                // Storage::create_couple invokes Couple::bind_groups, so no need to set m_couple here
            }
        } else {
            // TODO: m_couple != nullptr && m_couple->m_key != other.m_couple->m_key
            if (m_couple->get_key() != other.m_couple->get_key()) {
                BH_LOG(m_storage.get_app().get_logger(), DNET_LOG_ERROR,
                        "Group merge: unhandled case: group has moved from couple %s to couple %s",
                        m_couple->get_key().c_str(), other.m_couple->get_key().c_str());
            }
        }
    } else if (m_couple != nullptr) {
        BH_LOG(m_storage.get_app().get_logger(), DNET_LOG_ERROR,
                "Group merge: unhandled case: group has gone from couple %s", m_couple->get_key().c_str());
        // TODO
    }

    if (other.m_namespace != nullptr) {
        if (m_namespace == nullptr) {
            m_namespace = &m_storage.get_namespace(other.m_namespace->get_name());
        } else {
            // TODO: handle change of namespace
            if (m_namespace->get_name() != other.m_namespace->get_name()) {
                BH_LOG(m_storage.get_app().get_logger(), DNET_LOG_ERROR,
                        "Group merge: unhandled case: group has moved from namespace %s to namespace %s",
                        m_namespace->get_name().c_str(), other.m_namespace->get_name().c_str());
            }
        }
    }

    // TODO!!!
    if (m_couple != nullptr && m_namespace != nullptr)
        m_namespace->add_couple(*m_couple);

    // As of m_backends, it must have been initialized during
    // Storage merge => Node merge => Backend clone ctor
    if (m_backends.size() != other.m_backends.size()) {
        BH_LOG(m_storage.get_app().get_logger(), DNET_LOG_ERROR,
                "Internal inconsistency: Group merge: subject group has %lu backends, other has %lu",
                m_backends.size(), other.m_backends.size());
    }
}

bool Group::match(const Filter & filter, uint32_t item_types) const
{
    if ((item_types & Filter::Group) && !filter.groups.empty()) {
        if (!std::binary_search(filter.groups.begin(), filter.groups.end(), m_id))
            return false;
    }

    if ((item_types & Filter::Namespace) && !filter.namespaces.empty()) {
        Namespace *ns = m_namespace;

        if (ns == nullptr)
            return false;

        if (!std::binary_search(filter.namespaces.begin(), filter.namespaces.end(), ns->get_name()))
            return false;
    }

    if ((item_types & Filter::Couple) && !filter.couples.empty()) {
        if (m_couple == nullptr)
            return false;

        if (!std::binary_search(filter.couples.begin(), filter.couples.end(), m_couple->get_key()))
            return false;
    }

    bool check_nodes = (item_types & Filter::Node) && !filter.nodes.empty();
    bool check_backends = (item_types & Filter::Backend) && !filter.backends.empty();
    bool check_fs = (item_types & Filter::FS) && !filter.filesystems.empty();

    bool found_node = false;
    bool found_backend = false;
    bool found_fs = false;

    if (check_nodes || check_backends || check_fs) {
        for (Backend *backend : m_backends) {
            if (check_nodes && !found_node) {
                if (!std::binary_search(filter.nodes.begin(), filter.nodes.end(),
                            backend->get_node().get_key()))
                    found_node = true;
            }
            if (check_backends && !found_backend) {
                if (std::binary_search(filter.backends.begin(), filter.backends.end(),
                            backend->get_key()))
                    found_backend = true;
            }
            if (check_fs && !found_fs) {
                if (backend->get_fs() != nullptr && std::binary_search(filter.filesystems.begin(),
                            filter.filesystems.end(), backend->get_fs()->get_key()))
                    found_fs = true;
            }

            if (check_nodes == found_node && check_backends == found_backend && check_fs == found_fs)
                return true;
        }

        return false;
    }

    return true;
}

void Group::print_info(std::ostream & ostr) const
{
    ostr << "Group " << m_id << " {\n"
            "  couple:   [ ";

    if (m_couple != nullptr) {
        std::vector<int> group_ids;
        m_couple->get_group_ids(group_ids);
        for (size_t i = 0; i < group_ids.size(); ++i)
            ostr << group_ids[i] << ' ';
    }

    ostr << "]\n"
            "  backends: [ ";

    size_t i = 1;
    for (auto it = m_backends.begin(); it != m_backends.end(); ++it, ++i) {
        if (i != 1)
            ostr << "              ";

        ostr << (*it)->get_node().get_key() << '/' << (*it)->get_stat().backend_id;

        if (i < m_backends.size())
            ostr << '\n';
    }
    ostr << " ]\n"
            "  clean: " << std::boolalpha << m_clean << "\n"
            "  status_text: " << m_status_text << "\n"
            "  status: " << status_str(m_status) << "\n"
            "  metadata_process_start: " << m_metadata_process_start << "\n"
            "  metadata_process_time: " << m_metadata_process_time << "\n"
            "  frozen: " << m_frozen << "\n"
            "  version: " << m_version << "\n"
            "  namespace: ";

    if (m_namespace != nullptr)
        ostr << m_namespace->get_name() << '\n';
    else
        ostr << "<null>\n";

    ostr << "  service: {\n"
            "    migrating: " << m_service.migrating << "\n"
            "    job_id: '" << m_service.job_id << "'\n"
            "  }\n"
            "}";
}

void Group::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();

    writer.Key("id");
    writer.Uint64(m_id);

    if (m_couple != nullptr) {
        writer.Key("couple");
        writer.String(m_couple->get_key().c_str());
    }

    writer.Key("backends");
    writer.StartArray();
    for (Backend *backend : m_backends)
        writer.String(backend->get_key().c_str());
    writer.EndArray();

    writer.Key("status_text");
    writer.String(m_status_text.c_str());
    writer.Key("status");
    writer.String(status_str(m_status));
    writer.Key("frozen");
    writer.Bool(m_frozen);
    writer.Key("version");
    writer.Uint64(m_version);
    writer.Key("namespace");
    writer.String(m_namespace != nullptr ? m_namespace->get_name().c_str() : "");

    if (m_service.migrating || !m_service.job_id.empty()) {
        writer.Key("service");
        writer.StartObject();
        writer.Key("migrating");
        writer.Bool(m_service.migrating);
        writer.Key("job_id");
        writer.String(m_service.job_id.c_str());
        writer.EndObject();
    }

    writer.EndObject();
}

const char *Group::status_str(Status status)
{
    switch (status)
    {
    case INIT:
        return "INIT";
    case COUPLED:
        return "COUPLED";
    case BAD:
        return "BAD";
    case BROKEN:
        return "BROKEN";
    case RO:
        return "RO";
    case MIGRATING:
        return "MIGRATING";
    }
    return "UNKNOWN";
}