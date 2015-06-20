import msgpack

from mastermind.query import groups, namespaces, couples
from mastermind.service import ReconnectableService


class MastermindClient(object):
    """Provides python binding to mastermind cocaine application.

    Args:
      app_name:
        Cocaine application name, defaults to "mastermind2.26".
      **kwargs:
        Parameters for constructing ReconnactableService object which
        is used for cocaine requests.
    """

    DEFAULT_APP_NAME = 'mastermind2.26'

    def __init__(self, app_name=None, **kwargs):
        self.service = ReconnectableService(app_name=app_name or self.DEFAULT_APP_NAME,
                                            **kwargs)

    def request(self, handle, data, attempts=None, timeout=None):
        """Performs syncronous requests to mastermind cocaine application.

        Args:
          handle: API handle name.
          data: request data that will be serialized and sent.
        """
        return self.service.enqueue(
            handle, msgpack.packb(data),
            attempts=attempts, timeout=timeout).get()

    @property
    def groups(self):
        return groups.GroupsQuery(self)

    @property
    def namespaces(self):
        return namespaces.NamespacesQuery(self)

    @property
    def couples(self):
        return couples.CouplesQuery(self)
