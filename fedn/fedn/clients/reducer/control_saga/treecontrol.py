import asyncio
from collections import namedtuple
import random
from typing import List
from fedn.clients.reducer.control_saga.roundcontrol import CombinerNetwork, get_combiner_with_info, handle_unavailable_combiner
from fedn.clients.reducer.interfaces import CombinerInterface, CombinerUnavailableError
from fedn.clients.reducer.state import ReducerState
from fedn.combiner import Combiner, Availability_Status


# async def ping(self,combiners:List[CombinerInterface]):
#     for resp in asyncio.as_completed(map(lambda combiner: combiner.ping(), combiners)):
#     try:
#         combiner:CombinerInterface = await resp
#         combiner.get_model()
#     except CombinerUnavailableError as combiner_error:
#         handle_unavailable_combiner(combiner_error.combiner)

async def select_leafnodes(config, combiners: List[CombinerInterface]):
    # TODO not available_combiners handling

    # Get non booked combiners
    all_combiners = (get_combiner_with_info(combiner, combiner.is_available()) for combiner in combiners)
    requested_combiners = await asyncio.gather(*all_combiners)
    available_combiners = [parent for (parent, response) in requested_combiners if response == Availability_Status.FREE.value]

    # Select combiners to supervise
    selected = random.sample(available_combiners, config.get('leaf_numbers') or 2) # TODO put leaf number in config page
    selected_combiners = (get_combiner_with_info(combiner, combiner.book()) for combiner in selected)
    leaf_combiners = await asyncio.gather(*selected_combiners)

    leaf_combiners = [combiner for (combiner, response) in leaf_combiners if response == 'ack']

    if len(leaf_combiners) < (config.get('leaf_numbers') or 2):
        print('\n Not enough leafs \n')
    return leaf_combiners


def set_combiners_current_model_to_reducer_one():
    pass


def commit_to_local_model():
    pass


class TreeControl:
    """ 
    Reducer level round controller. 
    """
    # def __new__(cls):
    #     """ creates a singleton object, if it is not created,
    #     or else returns the previous singleton object"""
    #     if not hasattr(cls, 'instance'):
    #         cls.instance = super(RoundControl, cls).__new__(cls)
    #     return cls.instance

    def __init__(self, statestore, localCombiner: Combiner):
        self._state = ReducerState.setup
        Control = namedtuple("Control", 'idle')
        control = Control(lambda: self._state == ReducerState.idle)
        self.network = CombinerNetwork(control, statestore)
        self.localCombiner = localCombiner

    def start_with_plan(self, config):
        if not self._state == ReducerState.instructing:
            self._state = ReducerState.instructing
            try:
                asyncio.run(execute_plan(config, self.network, self.select_supervised_nodes))
            except:
                print("plan execution failed")
            self._state = ReducerState.idle

    def select_supervised_nodes(self, combiners: List[CombinerInterface]):
        data = {
            'supervisor': self.localCombiner.id,
            'worker': [combiner.name for combiner in combiners]
        }
        self.localCombiner.announce_client.announce_supervised_combiners(data)


async def execute_plan(config, network: CombinerNetwork, anounce_leaves):
    combiners = network.get_combiners()
    supervised_combiners = await select_leafnodes(config, combiners)
    anounce_leaves(supervised_combiners)
    await instruct_leaves(config, supervised_combiners)

    for round in range(1, int(config['rounds'] + 1)):
        round_meta = await supervise_round(config, round, network, supervised_combiners)
        print("REDUCER: Global round completed, new model: {}".format(round_meta["model_id"]), flush=True)


async def supervise_round(config, round, network: CombinerNetwork, supervised_combiners):

    return {'model_id': 'new'}

# ================
# Utility
# ================


async def instruct_leaves(config, supervised_combiners):
    for resp in asyncio.as_completed([combiner.instruct(config) for combiner in supervised_combiners]):
        try:
            await resp
        except CombinerUnavailableError as combiner_error:
            handle_unavailable_combiner(combiner_error.combiner)


def anounce_leaves(combiners: List[CombinerInterface]):
    pass
