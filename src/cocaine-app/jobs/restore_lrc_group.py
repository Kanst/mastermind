import logging

from infrastructure import infrastructure
from job import Job
from job_types import JobTypes
from tasks import (RecoverGroupDcTask, LrcRecoveryTask)
import storage


logger = logging.getLogger('mm.jobs')


class RestoreLrcGroupJob(Job):
    PARAMS = ('group', 'resources')

    def __init__(self, **kwargs):
        super(RestoreLrcGroupJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_RESTORE_LRC_GROUP_JOB

    def _set_resources(self):
        resources = {
            Job.RESOURCE_HOST_IN: [],
            Job.RESOURCE_HOST_OUT: [],
            Job.RESOURCE_FS: [],
        }

        self.resources = resources

    @staticmethod
    def _get_shard(group):
        group = storage.groups[group]
        couple = group.couple
        if couple.couple.lrc822v1_groupset is None:
            raise ValueError('Couple {} for group {} is not lrc'.format(couple, group))

        shard = None
        for s in storage.Lrc.Scheme822v1.INDEX_SHARD_INDICES:
            if couple.groups.index(group) in s:
                shard = s

        if shard is None:
            raise

        groups = []
        for index in shard:
            groups.append(couple.groups[index])

        return groups

    def create_tasks(self, processor):
        groups = self._get_shard(self.group)
        group_ids = set([self.group])
        group_ids.update(g.group_id for g in groups)
        group = storage.groups[self.group]
        couple = group.couple

        dnet_recover_cmd = infrastructure._recover_lrc_group_cmd(
            couple,
            groups,
            json_stats=True,
            trace_id=self.id[:16],
        )

        group = storage.groups[self.group]
        group_nb = group.node_backends[0]
        task = RecoverGroupDcTask.new(
            self,
            group=self.group,
            host=str(group_nb.node.host.addr),
            cmd=dnet_recover_cmd,
            json_stats=True,
            params={
                'node_backend': self.node_backend(
                    host=group_nb.node.host.addr,
                    port=group_nb.node.port,
                    backend_id=group_nb.backend_id,
                ),
                'group': str(self.group),
            }
        )
        self.tasks.append(task)

        recover_cmd = infrastructure._lrc_recovery_cmd(
            couple,
            couple.groups,
            part_size=couple.part_size,
            scheme=couple.scheme,
            trace_id=self.id[:16],
            json_stats=True,
        )

        task = LrcRecoveryTask.new(
            self,
            group=self.group,
            host=str(group_nb.node.host.addr),
            cmd=recover_cmd,
            json_stats=True,
            params={
                'node_backend': self.node_backend(
                    host=group_nb.node.host.addr,
                    port=group_nb.node.port,
                    backend_id=group_nb.backend_id,
                ),
                'group': str(self.group),
            }
        )
        self.tasks.append(task)


    @property
    def _involved_groups(self):
        # group_ids = set([self.group])
        # group_ids.update(g.group_id for g in self._get_shard(self.group))
        # return group_ids
        return []

    @property
    def _involved_couples(self):
        # group = storage.groups[self.group]
        # couple = group.couple
        # return [couple]
        return []
