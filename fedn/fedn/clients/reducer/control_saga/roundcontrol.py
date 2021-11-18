from collections import namedtuple
import os
from fedn.clients.reducer.network import Network
from fedn.clients.reducer.statestore.mongoreducerstatestore import MongoReducerStateStore
from fedn.utils.helpers import get_helper

from abc import abstractmethod
import asyncio
from copy import deepcopy
from typing import List
from fedn.clients.reducer.interfaces import CombinerInterface, CombinerUnavailableError

from fedn.clients.reducer.state import ReducerState
from fedn.common import net
from fedn.common.tracer.mongotracer import ResourceTracker
import base64

class UnsupportedStorageBackend(Exception):
    pass


class MisconfiguredStorageBackend(Exception):
    pass

class CombinerNetwork(Network):

    async def is_combiners_available(self):
        return True  # TODO

    def get_combiners(self) -> List[CombinerInterface]:
        data = self._statestore.get_combiners() or []
        return [CombinerInterface(c['parent'], c['name'], c['address'], c['port'],
                                  base64.b64decode(c['certificate']),
                                  base64.b64decode(c['key']), c['ip']) for c in data]

    async def share_plan(self, plan):
        pass

    def get_latest_model(self):
        return self._statestore.get_latest()

    def get_framework(self):
        return self._statestore.get_framework()

    async def sync_combiners(self, combiners, model_id):
        for resp in asyncio.as_completed(map(lambda combiner: combiner.a_set_model_id(model_id), combiners)):
            try:
                await resp
            except CombinerUnavailableError as combiner_error:
                handle_unavailable_combiner(combiner_error.combiner)

    async def execute_training(self, combiners, compute_plan):
        for resp in asyncio.as_completed(map(lambda combiner: combiner.a_start(compute_plan), combiners)):
            try:
                await resp
            except CombinerUnavailableError as combiner_error:
                handle_unavailable_combiner(combiner_error.combiner)

    async def get_out_of_sync_combiners(self)->List[CombinerInterface]:
        combiners = self.get_combiners()
        latest_model = self.get_latest_model()

        model_ids = (get_combiner_with_info(combiner, combiner.a_get_model_id()) for combiner in combiners)
        combiner_with_model = await asyncio.gather(*model_ids)
        out_of_sync = [parent for (parent, model_id) in combiner_with_model if not model_id or model_id and latest_model != model_id]
        return out_of_sync
    
    async def get_participating_combiners(self,compute_plan)->List[CombinerInterface]:
        combiners = self.get_combiners()

        if not combiners:
            raise Exception("No combiners available")

        reports = (get_combiner_with_info(combiner, combiner.a_report()) for combiner in combiners)
        participating_combiners = [parent for (parent, combiner_state) in await asyncio.gather(*reports)
                                   if combiner_state and check_round_participation_policy(compute_plan, combiner_state)]
        return participating_combiners



    def get_store(self):
        return StateStore(self._statestore)


    

## Main functions

async def execute_plan(config, network: CombinerNetwork):
    for round in range(1, int(config['rounds'] + 1)):
        round_meta = await supervise_round(config, round, network)
        print("REDUCER: Global round completed, new model: {}".format(round_meta["model_id"]), flush=True)


async def supervise_round(config, round, network: CombinerNetwork):
    statestore_config = network._statestore.get_config()
    with ResourceTracker(round,statestore_config['mongo_config'], statestore_config['network_id']) as tracker_control:
        round_meta = {'round_id': round}
        tracker_control.set_round_meta(round_meta)

        if not await network.is_combiners_available():
            raise Exception("No combiners connected!")



        compute_plan = deepcopy(config)
        compute_plan['rounds'] = 1
        compute_plan['round_id'] = round
        compute_plan['task'] = 'training'
        compute_plan['model_id'] = network.get_latest_model()
        compute_plan['helper_type'] = network.get_framework()

        round_meta['compute_plan'] = compute_plan
        participating_combiners = await network.get_participating_combiners(compute_plan)

        round_start = check_round_start_policy(participating_combiners)
        print("CONTROL: round start policy met, participating combiners {}".format(round_start), flush=True)
        if not round_start:
            raise Exception("CONTROL: Round start policy not met, skipping round!")

        # ===========
        # Training
        # ===========

        await network.sync_combiners(participating_combiners, compute_plan['model_id'])
        await network.execute_training(participating_combiners, compute_plan)
        tracker_control.set_combiner_time()

        updated = await network.get_out_of_sync_combiners()
        check_round_validity_policy(updated)

        # ===========
        # reduce model
        # ===========

        helper = get_helper(compute_plan['helper_type'])
        model, data = await reduce_global_model(updated, helper)
        if data:
            round_meta['reduce'] = data

        await commit_model(model, round_meta, helper, network.get_store())

        # ===========
        # validation
        # ===========

        combiner_config = deepcopy(config)
        combiner_config['model_id'] = network.get_latest_model()
        combiner_config['task'] = 'validation'
        await network.execute_training(participating_combiners, combiner_config)

        tracker_control.set_round_meta(round_meta)


    if not round_meta.get("model_id"): round_meta["model_id"] = "same"
    return round_meta


async def reduce_global_model(updated, helper):
    try:
        model, data = await a_reduce(updated, helper)
    except Exception as e:
        print("CONTROL: Failed to reduce models from combiners: {}".format(updated), flush=True)
        print(e, flush=True)
        return None, None
    print("DONE", flush=True)
    return model, data


async def commit_model(model, round_meta, helper, store):
    if model is not None:
        import time
        import uuid
        # Commit to model ledger
        tic = time.time()
        model_id = uuid.uuid4()
        commit(model_id, model, helper, store)
        round_meta['time_commit'] = time.time() - tic
        round_meta['model_id'] = model_id
    else:
        # print("REDUCER: failed to update model in round with config {}".format(config), flush=True)
        return None, round_meta
    print("DONE", flush=True)


class RoundControl():
    """ 
    Reducer level round controller. 
    """
    # def __new__(cls):
    #     """ creates a singleton object, if it is not created,
    #     or else returns the previous singleton object"""
    #     if not hasattr(cls, 'instance'):
    #         cls.instance = super(RoundControl, cls).__new__(cls)
    #     return cls.instance


    def __init__(self, statestore, localCombiner):
        self._state = ReducerState.setup
        Control = namedtuple("Control", 'idle')
        control = Control(lambda: self._state == ReducerState.idle)
        self.network = CombinerNetwork(control, statestore)

    # @abstractmethod
    # def get_state(self):
    #     return self._state == ReducerState.instructing


    def start_with_plan(self, config):
        if not self._state == ReducerState.instructing:
            self._state = ReducerState.instructing
            try:
                asyncio.run(execute_plan(config, self.network))
            except:
                print("plan execution failed")
            self._state = ReducerState.idle

class StateStore():
    def __init__(self, statestore:MongoReducerStateStore):
        self.__state = ReducerState.setup
        self._statestore = statestore
        if self._statestore.is_inited():
            self._network = Network(self, statestore)

        try:
            config = self._statestore.get_storage_backend()
        except:
            print("REDUCER CONTROL: Failed to retrive storage configuration, exiting.", flush=True)
            raise MisconfiguredStorageBackend()
        if not config:
            print("REDUCER CONTROL: No storage configuration available, exiting.", flush=True)
            raise MisconfiguredStorageBackend()

        if config['storage_type'] == 'S3':
            from fedn.common.storage.s3.s3repo import S3ModelRepository
            self.model_repository = S3ModelRepository(config['storage_config'])
        else:
            print("REDUCER CONTROL: Unsupported storage backend, exiting.", flush=True)
            raise UnsupportedStorageBackend()

        if self._statestore.is_inited():
            self.__state = ReducerState.idle

    def set_file_as_model(self,outfile_name):
        return self.model_repository.set_model(outfile_name, is_file=True)

    def set_latest_model(self,model_id): 
        return self._statestore.set_latest(model_id)


#############################
# HELPER
#############################


def check_round_participation_policy(compute_plan, combiner_state):
    """ Evaluate reducer level policy for combiner round-paarticipation.
        This is a decision on ReducerControl level, additional checks
        applies on combiner level. Not all reducer control flows might
        need or want to use a participation policy.  """

    if compute_plan['task'] == 'training':
        nr_active_clients = int(combiner_state['nr_active_trainers'])
    elif compute_plan['task'] == 'validation':
        nr_active_clients = int(combiner_state['nr_active_validators'])
    else:
        print("Invalid task type!", flush=True)
        return False

    if int(compute_plan['clients_required']) <= nr_active_clients:
        return True
    else:
        return False


async def get_combiner_with_info(combiner, info):
    try:
        response = await info
    except CombinerUnavailableError as combiner_error:
        handle_unavailable_combiner(combiner)
        response = None
    return(combiner, response)


def check_round_start_policy(combiners):
    """ Check if the overall network state meets the policy to start a round. """
    if len(combiners) > 0:
        return True
    else:
        return False


def handle_unavailable_combiner(combiner):
    """ This callback is triggered if a combiner is found to be unresponsive. """
    # TODO: Implement strategy to handle the case.
    print("REDUCER CONTROL: Combiner {} unavailable.".format(combiner.name), flush=True)


def check_round_validity_policy(combiners):
    """
    At the end of the round, before committing a model to the model ledger,
    we check if a round validity policy has been met. This can involve
    e.g. asserting that a certain number of combiners have reported in an
    updated model, or that criteria on model performance have been met.
    """
    print("Checking round validity policy...", flush=True)
    if combiners == []:
        print("REDUCER CONTROL: Round invalid!", flush=True)
    else:
        print("Round valid.")
    return not combiners == []

######################
# reduce
######################


async def a_reduce(combiners: List[CombinerInterface], helper):
    """ Combine current models at Combiner nodes into one global model. """
    import time
    meta = {}
    meta['time_fetch_model'] = 0.0
    meta['time_load_model'] = 0.0
    meta['time_aggregate_model'] = 0.0

    i = 1
    model = None
    for combiner in combiners:

        # TODO: Handle inactive RPC error in get_model and raise specific error
        try:
            tic = time.time()
            data = combiner.get_model()
            meta['time_fetch_model'] += (time.time() - tic)
        except:
            data = None

        if data is not None:
            try:
                tic = time.time()
                model_buffer = await combiner.a_get_model()
                model_str = model_buffer.getbuffer()
                model_next = helper.load_model_from_BytesIO(model_str)
                meta['time_load_model'] += (time.time() - tic)
                tic = time.time()
                if model:
                    model = helper.increment_average(model, model_next, i)
                else:
                    model = model_next
                meta['time_aggregate_model'] += (time.time() - tic)
            except:
                tic = time.time()
                model = helper.load_model_from_BytesIO(data.getbuffer())
                meta['time_aggregate_model'] += (time.time() - tic)
            i = i + 1

    return model, meta


def commit(model_id, model, helper,store:StateStore):
    """ Commit a model to the global model trail. The model commited becomes the lastest consensus model. """

    if model is not None:
        print("Saving model to disk...", flush=True)
        outfile_name = helper.save_model(model)
        print("DONE", flush=True)
        print("Uploading model to Minio...", flush=True)

        model_id = store.set_file_as_model(outfile_name)
        print("DONE", flush=True)
        os.unlink(outfile_name)

    store.set_latest_model(model_id)
