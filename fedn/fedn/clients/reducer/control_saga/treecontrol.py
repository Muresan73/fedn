import asyncio
import base64
from collections import namedtuple
import random
from typing import List
from fedn.clients.reducer.control_saga.roundcontrol import CombinerNetwork, get_combiner_with_info, handle_unavailable_combiner
from fedn.clients.reducer.interfaces import CombinerInterface, CombinerUnavailableError
from fedn.clients.reducer.state import ReducerState
from fedn.combiner import Combiner, Availability_Status
from fedn.clients.reducer.control_saga.roundcontrol import supervise_round

async def select_leafnodes(config, combiners: List[CombinerInterface]):
    # TODO not available_combiners handling

    # Get non booked combiners
    all_combiners = (get_combiner_with_info(combiner, combiner.is_available()) for combiner in combiners)
    requested_combiners = await asyncio.gather(*all_combiners)
    available_combiners = [parent for (parent, response) in requested_combiners if response == Availability_Status.FREE.value]

    # Select combiners to supervise
    if len(available_combiners) > config.get('leaf_numbers'):
        selected = random.sample(available_combiners, config.get('leaf_numbers')) # TODO put leaf number in config page
    else:
        selected = available_combiners
    selected_combiners = (get_combiner_with_info(combiner, combiner.book()) for combiner in selected)
    leaf_combiners = await asyncio.gather(*selected_combiners)

    leaf_combiners = [combiner for (combiner, response) in leaf_combiners if response == 'ack']

    if len(leaf_combiners) < (config.get('leaf_numbers')):
        print('\n Not enough leafs \n')
    return leaf_combiners

class Error(Exception):
    pass

class TreeCombinerNetwork(CombinerNetwork):
    def set_controlled_nodes(self,combiner_list:List[CombinerInterface]):
        self.combiner_list = combiner_list

    def get_combiners(self) -> List[CombinerInterface]:
        return self.combiner_list
        
    def get_all_combiners(self) -> List[CombinerInterface]:
        data = self._statestore.get_combiners() or []
        return [CombinerInterface(c['parent'], c['name'], c['address'], c['port'],
                                  base64.b64decode(c['certificate']),
                                  base64.b64decode(c['key']), c['ip']) for c in data]
    
    def get_latest_model(self):
        if hasattr(self, 'latest_model'):
            return self.latest_model
        else: 
            raise Error("No latest model set")

    def set_latest_model(self, latest_model):
        self.latest_model = latest_model
    
    def get_global_latest_model(self):
        return self._statestore.get_latest()

class TreeControl:
    """ 
    Reducer level round controller. 
    """

    def __init__(self, statestore, localCombiner: Combiner):
        self._state = ReducerState.setup
        Control = namedtuple("Control", 'idle')
        control = Control(lambda: self._state == ReducerState.idle)
        self.network = TreeCombinerNetwork(control, statestore)
        self.localCombiner = localCombiner

    def start_with_plan(self, config, loop=None):
        if not self._state == ReducerState.instructing:
            self._state = ReducerState.instructing
            self.network._statestore.copy_global_model()
            try:
                if loop:
                    asyncio.run_coroutine_threadsafe(execute_plan(config, self.network),loop)
                else:
                    asyncio.run(execute_plan(config, self.network))
                self.network._statestore.update_global_model()

            except:
                print("plan execution failed")
            
            self._state = ReducerState.idle

    def select_supervised_nodes(self, combiners: List[CombinerInterface]):
        data = {
            'supervisor': self.localCombiner.id,
            'worker': [combiner.name for combiner in combiners]
        }
        self.localCombiner.announce_client.announce_supervised_combiners(data)

    def request_model_update(self,model_id):
        self.network.set_latest_model(model_id)

    def build_supervisor_tree(self,config={'leaf_numbers':2},reducer_name=None,loop=None):
        combiners = self.network.get_all_combiners()
        if loop:
            supervised_combiners = asyncio.run_coroutine_threadsafe( select_leafnodes(config, combiners),loop).result()
        else:
            supervised_combiners = asyncio.run( select_leafnodes(config, combiners))
        if reducer_name:
            reducer_combiner = next(combiner for combiner in combiners if combiner.name == reducer_name)
            supervised_combiners.append(reducer_combiner)
        self.network.set_controlled_nodes(supervised_combiners)
        if len(supervised_combiners) > 0 :
            self.select_supervised_nodes(supervised_combiners)
            print("booked peers",[x.name for x in supervised_combiners])
            
    async def run_one_round(self, config):
        if self.is_supervisor() :
            return await supervise_round(config,config['rounds'], self.network)
        else:
            return None, None

    def is_supervisor(self):
        supervised_combiners = self.network.get_combiners()
        return len(supervised_combiners) > 0



async def execute_plan(config, network: TreeCombinerNetwork):
    print("Start execution")
    supervised_combiners = network.get_combiners()
    if len(supervised_combiners) > 0 :

        for round in range(1, int(config['rounds'] + 1)):

            network.set_latest_model(network.get_global_latest_model())
            round_meta, model = await supervise_round(config, round, network)
            print("REDUCER: Global round completed, new model: {}".format(round_meta["model_id"]), flush=True)
    else:
        print("no combiners to supervise")



# ================
# Utility
# ================

