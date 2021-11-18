from abc import ABC, abstractmethod, abstractproperty, ABCMeta
from typing import Any, List

from fedn.clients.reducer.statestore.reducerstatestore import ReducerStateStore
from fedn.clients.reducer.state import ReducerState
from fedn.clients.reducer.interfaces import CombinerInterface


class AbstractNetwork:
    @abstractmethod
    def add_new_combiner(self,parent, name, address, port, certificate, key, remote_addr):
        #TODO add combiner with certificate to a reducer
        pass

    @abstractmethod
    def get_combiners(self) -> List[CombinerInterface]:
        pass

class AbstractReducerControl:
   
    @abstractmethod
    def commit(self,filename, model_buffer):
        pass
    @abstractmethod
    def delete_bucket_objects(self):
        pass
    @abstractmethod
    def drop_models(self):
        pass
    @abstractmethod
    def assign_combiner(self,name, combiner_preferred,remote_address):
        pass

    @abstractmethod
    def get_compute_context(self,) -> str:
        '''
        used for check if contect exists
        
        '''
        pass
    @abstractmethod
    def get_compute_package(self,name):
        # TODO restructire functionality
        pass
    @abstractmethod
    def get_events(self,)->List:
        '''
        return pymongo.cursor.Cursor object
        iterable dictionary
        '_id, sender, status, logLevel, data, timestamp, type, correlationId, extra'
        '''
        pass
    @abstractmethod
    def get_first_model(self,) -> List[str]:
        '''
        return list of model ids
        '''
        pass
    @abstractmethod
    def get_helper(self,):
        '''
        return fedn.utils.helpers.get_helper based on the framework in use
        '''
        pass
    @abstractmethod
    def get_latest_model(self,) -> str:
        '''
        used for check if initial model set
        return model ids
        
        '''
        pass

    @abstractmethod
    def start(self,config):
        pass

    @abstractmethod
    def get_model_info(self,):
        '''
        return dict with { model_id : time_stamp }
        '''
        pass
    @abstractmethod
    def instruct(self,config):
        pass

    @abstractmethod
    def set_compute_context(self,filename, file_path):
        # TODO sending files and downloading from the context endpoint
        pass

    @abstractmethod
    def state(self,) -> ReducerState:
        pass

    @abstractmethod
    def get_client_info(self,) -> List:
        '''
        list dict
        keys:
        '_id, name, combiner_preferred, combiner, ip, status, updated_at'
        
        '''
        pass

    @abstractproperty
    def network(self) -> AbstractNetwork:
        network:Any = None
        return network

    @abstractproperty
    def statestore(self) -> ReducerStateStore:
        store:Any = None
        return store

    @abstractmethod
    def combiner_status(self) -> List:
        '''
        return list of dict with keys:
        'nr_active_clients, model_id, nr_unprocessed_compute_plans, name'
        '''
        pass

    def update_client_data(self, client_data, status, role):
        pass

    def drop_control(self):
        pass
    def delete_model_trail(self): 
        pass
    def get_framework(self):
        pass
    def set_framework(self,helper_type:str):
        pass
    def idle(self):
        pass
    def add_combiner(self, name, address, port, remote_address,certificate_manager,rest_type):
        pass