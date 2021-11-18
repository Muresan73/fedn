import copy
import time
import base64
from typing import List, Optional
from fedn.clients.reducer.abstract_control import AbstractNetwork
from fedn.clients.reducer.statestore.mongoreducerstatestore import MongoReducerStateStore

from fedn.clients.reducer.interfaces import CombinerInterface
from fedn.clients.reducer.interfaces import CombinerUnavailableError


class Network():
    """ FEDn network. """

    def __init__(self, control, statestore:MongoReducerStateStore):
        """ """
        self._statestore = statestore
        self.control = control
        self.id = statestore.network_id

    @classmethod
    def from_statestore(self, network_id):
        """ """

    def get_combiner(self, name) -> Optional[CombinerInterface]:
        """

        :param name:
        :return:
        """
        
        c = self._statestore.get_combiner(name)
        if not c:
            return print("Combiner not available with name: {}".format(name))
        return CombinerInterface(c['parent'], c['name'], c['address'], c['port'], base64.b64decode(c['certificate']),
                                  base64.b64decode(c['key']), c['ip'])

    def get_combiners(self)->List[CombinerInterface]:
        """

        :return:
        """
        # TODO: Read in combiners from statestore
        data = self._statestore.get_combiners()
        combiners = []
        for c in data:
            combiners.append(
                CombinerInterface(c['parent'], c['name'], c['address'], c['port'], base64.b64decode(c['certificate']),
                                  base64.b64decode(c['key']), c['ip']))

        return combiners

    def add_new_combiner(self, parent, name, address, port, certificate, key, remote_addr):
        combiner = CombinerInterface(parent, name, address, port, certificate, key, remote_addr)
        self.add_combiner(combiner)

    def add_combiner(self, combiner):
        """

        :param combiner:
        :return:
        """
        if not self.control.idle():
            print("Reducer is not idle, cannot add additional combiner.")
            return

        if self.find(combiner.name):
            return

        print("adding combiner {}".format(combiner.name), flush=True)
        self._statestore.set_combiner(combiner.to_dict())

    def add_client(self, client):
        """ Add a new client to the network. 

        :param client:
        :return:
        """

        if self.find_client(client['name']):
            return 

        print("adding client {}".format(client['name']), flush=True)
        self._statestore.set_client(client)

    def remove(self, combiner):
        """

        :param combiner:
        :return:
        """
        if not self.control.idle():
            print("Reducer is not idle, cannot remove combiner.")
            return
        self._statestore.delete_combiner(combiner.name)

    def find(self, name):
        """

        :param name:
        :return:
        """
        combiners = self.get_combiners()
        for combiner in combiners:
            if name == combiner.name:
                return combiner
        return None

    def find_client(self, name):
        """

        :param name:
        :return:
        """
        ret = self._statestore.get_client(name)
        return ret

    def describe(self):
        """ """
        network = []
        for combiner in self.get_combiners():
            try:
                network.append(combiner.report())
            except CombinerUnavailableError:
                # TODO, do better here.
                pass
        return network

    def check_health(self):
        """ """
        pass
