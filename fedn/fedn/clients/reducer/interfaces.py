from collections import namedtuple
import json
from typing import Any, Sequence, Tuple

import fedn.common.net.grpc.fedn_pb2 as fedn
import fedn.common.net.grpc.fedn_pb2_grpc as rpc
import grpc


class CombinerUnavailableError(Exception):
    def __init__(self, combiner=None):
        self.combiner = combiner


class Channel:

    def __init__(self, address, port, certificate):
        self.address = address
        self.port = port
        self.certificate = certificate
        if self.certificate:
            import copy
            credentials = grpc.ssl_channel_credentials(root_certificates=copy.deepcopy(certificate))
            self.channel = grpc.secure_channel('{}:{}'.format(self.address, str(self.port)), credentials)
        else:
            self.channel = grpc.insecure_channel('{}:{}'.format(self.address, str(self.port)))

    def get_channel(self):
        """

        :return:
        """
        import copy
        return copy.copy(self.channel)


class Async_Channel:
    def __init__(self, address, port, certificate):

        ChannelArgumentType = Sequence[Tuple[str, Any]]
        retry_config = json.dumps({
            "methodConfig": [{
                "name": [{
                    "service": "Control.Report"
                },
                    {
                    "service": "Control.Start"
                },
                    {
                    "service": "Control.Configure"
                },
                ],
                "retryPolicy": {
                    "maxAttempts": 5,
                    "initialBackoff": "0.1s",
                    "maxBackoff": "10s",
                    "backoffMultiplier": 2,
                    "retryableStatusCodes": ["UNAVAILABLE"],
                },
            }]
        })
        CHANNEL_CONFIG: ChannelArgumentType = [('grpc.service_config', retry_config)]

        self.address = address
        self.port = port
        self.certificate = certificate
        if self.certificate:
            import copy
            credentials = grpc.ssl_channel_credentials(root_certificates=copy.deepcopy(certificate))
            self.channel = grpc.aio.secure_channel('{}:{}'.format(
                self.address, str(self.port)), credentials, options=CHANNEL_CONFIG)
        else:
            self.channel = grpc.aio.insecure_channel('{}:{}'.format(
                self.address, str(self.port)), options=CHANNEL_CONFIG)

    def get_channel(self):
        """

        :return:
        """
        import copy
        return copy.copy(self.channel)


class CombinerInterface:
    """

    """

    def __init__(self, parent, name, address, port, certificate=None, key=None, ip=None, config=None):
        self.parent = parent
        self.name = name
        self.address = address
        self.port = port
        self.certificate = certificate
        self.key = key
        self.ip = ip

        if not config:
            self.config = {
                'max_clients': 8
            }
        else:
            self.config = config

    @classmethod
    def from_statestore(statestore, name):
        """ """

    @classmethod
    def from_json(combiner_config):
        """

        :return:
        """
        return CombinerInterface(**combiner_config)

    def to_dict(self):
        """

        :return:
        """
        import base64
        cert_b64 = base64.b64encode(self.certificate)
        key_b64 = base64.b64encode(self.key)

        data = {
            'parent': self.parent,
            'name': self.name,
            'address': self.address,
            'port': self.port,
            'certificate': str(cert_b64).split('\'')[1],
            'key': str(key_b64).split('\'')[1],
            'ip': self.ip
        }

        try:
            data['report'] = self.report()
        except CombinerUnavailableError:
            data['report'] = None

        return data

    def to_json(self):
        """

        :return:
        """
        return json.dumps(self.to_dict())

    def report(self, config=None):
        """

        :param config:
        :return:
        """
        channel = Channel(self.address, self.port, self.certificate).get_channel()
        control = rpc.ControlStub(channel)
        request = fedn.ControlRequest()
        try:
            response = control.Report(request)
            data = {}
            for p in response.parameter:
                data[p.key] = p.value
            return data
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError
            else:
                raise

    async def a_report(self, config=None):
        channel = Async_Channel(self.address, self.port, self.certificate).get_channel()
        control = rpc.ControlStub(channel)
        request = fedn.ControlRequest()
        try:
            response = await control.Report(request)
            data = {}
            for p in response.parameter:
                data[p.key] = p.value
            return data
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError
            else:
                raise

    def configure(self, config=None):
        """

        :param config:
        """
        if not config:
            config = self.config
        channel = Channel(self.address, self.port, self.certificate).get_channel()
        control = rpc.ControlStub(channel)

        request = fedn.ControlRequest()
        for key, value in config.items():
            p = request.parameter.add()
            p.key = key
            p.value = str(value)

        try:
            response = control.Configure(request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError
            else:
                raise

    def start(self, config):
        """

        :param config:
        :return:
        """
        channel = Channel(self.address, self.port, self.certificate).get_channel()
        control = rpc.ControlStub(channel)
        request = fedn.ControlRequest()
        request.command = fedn.Command.START
        for k, v in config.items():
            p = request.parameter.add()
            p.key = str(k)
            p.value = str(v)

        try:
            response = control.Start(request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError
            else:
                raise

        print("Response from combiner {}".format(response.message))
        return response

    def instruct(self, config):
        """

        :param config:
        :return:
        """
        channel = Channel(self.address, self.port, self.certificate).get_channel()
        control = rpc.ControlStub(channel)
        request = fedn.InstructConfig()
        for k, v in config.items():
            setattr(request, k, v)

        try:
            response = control.Instruct(request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError
            else:
                raise

        print("Response from combiner {}".format(response.message))
        return response
    
    async def a_instruct(self, config):
        """

        :param config:
        :return:
        """
        channel = Async_Channel(self.address, self.port, self.certificate).get_channel()
        control = rpc.ControlStub(channel)
        request = fedn.InstructConfig()
        for k, v in config.items():
            setattr(request, k, v)

        try:
            response = await control.Instruct(request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError
            else:
                raise

        print("Response from combiner {}".format(response.message))
        return response

    async def a_start(self, config):
        """

        :param config:
        :return:
        """
        channel = Async_Channel(self.address, self.port, self.certificate).get_channel()
        control = rpc.ControlStub(channel)
        request = fedn.ControlRequest()
        request.command = fedn.Command.START
        for k, v in config.items():
            p = request.parameter.add()
            p.key = str(k)
            p.value = str(v)

        try:
            response = await control.Start(request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError(self)
            else:
                raise

        print("Response from combiner {}".format(response.message))
        return response

    def set_model_id(self, model_id):
        """

        :param model_id:
        """
        channel = Channel(self.address, self.port, self.certificate).get_channel()
        control = rpc.ControlStub(channel)
        request = fedn.ControlRequest()
        p = request.parameter.add()
        p.key = 'model_id'
        p.value = str(model_id)

        try:
            response = control.Configure(request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError
            else:
                raise

    async def a_set_model_id(self, model_id):
        """

        :param model_id:
        """
        channel = Async_Channel(self.address, self.port, self.certificate).get_channel()
        control = rpc.ControlStub(channel)
        request = fedn.ControlRequest()
        p = request.parameter.add()
        p.key = 'model_id'
        p.value = str(model_id)

        try:
            response = await control.Configure(request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError(self)
            else:
                raise

    def get_model_id(self):
        """

        :return:
        """
        channel = Channel(self.address, self.port, self.certificate).get_channel()
        reducer = rpc.ReducerStub(channel)
        request = fedn.GetGlobalModelRequest()
        try:
            response = reducer.GetGlobalModel(request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError
            else:
                raise

        return response.model_id

    async def a_get_model_id(self):
        """

        :return:
        """
        channel = Async_Channel(self.address, self.port, self.certificate).get_channel()
        reducer = rpc.ReducerStub(channel)
        request = fedn.GetGlobalModelRequest()
        try:
            response = await reducer.GetGlobalModel(request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError(combiner=self)
            else:
                raise

        return response.model_id

    def get_model(self, id=None):
        """ Retrive the model bundle from a combiner. """

        channel = Channel(self.address, self.port, self.certificate).get_channel()
        modelservice = rpc.ModelServiceStub(channel)

        if not id:
            id = self.get_model_id()

        from io import BytesIO
        data = BytesIO()
        data.seek(0, 0)

        parts = modelservice.Download(fedn.ModelRequest(id=id))
        for part in parts:
            if part.status == fedn.ModelStatus.IN_PROGRESS:
                data.write(part.data)
            if part.status == fedn.ModelStatus.OK:
                return data
            if part.status == fedn.ModelStatus.FAILED:
                return None

    async def a_get_model(self, id=None):
        """ Retrive the model bundle from a combiner. """

        channel = Async_Channel(self.address, self.port, self.certificate).get_channel()
        modelservice = rpc.ModelServiceStub(channel)

        if not id:
            id = await self.a_get_model_id()

        from io import BytesIO
        data = BytesIO()
        data.seek(0, 0)

        parts = modelservice.Download(fedn.ModelRequest(id=id))
        async for part in parts:
            if part.status == fedn.ModelStatus.IN_PROGRESS:
                data.write(part.data)
            if part.status == fedn.ModelStatus.OK:
                return data
            if part.status == fedn.ModelStatus.FAILED:
                return None

    async def a_get_current_model(self, id=None):
        """ Retrive the model bundle from a combiner. """

        channel = Async_Channel(self.address, self.port, self.certificate).get_channel()
        modelservice = rpc.ModelServiceStub(channel)

        if not id:
            id = await self.a_get_model_id()

        from io import BytesIO
        data = BytesIO()
        data.seek(0, 0)

        parts = modelservice.Download(fedn.ModelRequest(id=id))
        async for part in parts:
            if part.status == fedn.ModelStatus.IN_PROGRESS:
                data.write(part.data)
            if part.status == fedn.ModelStatus.OK:
                return data
            if part.status == fedn.ModelStatus.FAILED:
                return None

    def allowing_clients(self):
        """

        :return:
        """
        channel = Channel(self.address, self.port, self.certificate).get_channel()
        connector = rpc.ConnectorStub(channel)
        request = fedn.ConnectionRequest()

        try:
            response = connector.AcceptingClients(request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError
            else:
                raise
        if response.status == fedn.ConnectionStatus.NOT_ACCEPTING:
            return False
        if response.status == fedn.ConnectionStatus.ACCEPTING:
            return True
        if response.status == fedn.ConnectionStatus.TRY_AGAIN_LATER:
            return False

        return False

    async def is_available(self):
        """

        :param model_id:
        """
        channel = Async_Channel(self.address, self.port, self.certificate).get_channel()
        control = rpc.ControlStub(channel)
        request = fedn.ControlRequest()

        try:
            response = await control.Availability(request)
            return response.message
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError(self)
            else:
                raise

    async def book(self):
        """

        :param model_id:
        """
        channel = Async_Channel(self.address, self.port, self.certificate).get_channel()
        control = rpc.ControlStub(channel)
        request = fedn.ControlRequest()

        try:
            response = await control.Book(request)
            return response.message
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                raise CombinerUnavailableError(self)
            else:
                raise


class ReducerInferenceInterface:
    """

    """

    def __init__(self):
        self.model_wrapper = None

    def set(self, model):
        """

        :param model:
        """
        self.model_wrapper = model

    def infer(self, params):
        """

        :param params:
        :return:
        """
        results = None
        if self.model_wrapper:
            results = self.model_wrapper.infer(params)

        return results
