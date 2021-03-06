import asyncio
import inspect
import json
import logging
from inspect import Signature
from logging import getLogger
from typing import Callable, Any, Optional, List, Type, Dict

import aio_pika
from aio_pika import Connection, Channel, Queue
from pydantic import BaseModel, Field

logger = getLogger(__name__)


class Listener(BaseModel):
    queue_name: str
    queue: Optional[Queue] = None
    channel: Optional[Channel] = None
    callback: Callable[..., Any]
    serializers: Dict[str, Type[BaseModel]] = {}
    message_param_name: Optional[str] = None
    queue_param_name: Optional[str] = None
    channel_param_name: Optional[str] = None

    # param_injectors = Field({
    #     aio_pika.IncomingMessage: lambda self, msg: msg,
    #     aio_pika.Queue: lambda self, msg: self.queue,
    #     aio_pika.Channel: lambda self, msg: self.channel
    # }, const=True)

    class Config:
        arbitrary_types_allowed = True

    def create_kwargs(self, message: aio_pika.IncomingMessage) -> Dict[str, Any]:
        kwargs = dict()

        # Message param
        if self.message_param_name:
            kwargs[self.message_param_name] = message

        # Queue param
        if self.queue_param_name:
            kwargs[self.queue_param_name] = self.queue

        # Channel param
        if self.channel_param_name:
            kwargs[self.channel_param_name] = self.channel

        if len(self.serializers) == 1:
            name, serializer = list(self.serializers.items())[0]
            model = serializer.parse_raw(message.body)
            kwargs[name] = model

        if len(self.serializers) > 1:
            json_body = json.loads(message.body)
            for name, serializer in self.serializers.items():
                model = serializer.parse_raw(json_body[name])
                kwargs[name] = model

        return kwargs

    async def listen(self, channel: Channel):
        self.channel = channel
        queue = await channel.declare_queue(
            self.queue_name,
            auto_delete=True
        )
        self.queue = queue

        logging.info(f"Starting to consume messages from {self.queue_name} queue.")
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                logging.debug(f"Received a message in {self.queue_name} queue.")
                async with message.process():
                    try:
                        kwargs = self.create_kwargs(message)
                        asyncio.create_task(self.callback(**kwargs))
                    except Exception as e:
                        logger.exception("An error was raised:\n")


def create_rabbit_listener(queue_name: str, func: Callable[..., Any]) -> Listener:
    values: Dict[str, Any] = dict(
        queue_name=queue_name,
        callback=func,
        serializers={}
    )

    signature = Signature.from_callable(func)
    for param in signature.parameters.values():
        if issubclass(param.annotation, aio_pika.Message):
            values['message_param_name'] = param.name
        elif issubclass(param.annotation, aio_pika.Queue):
            values['queue_param_name'] = param.name
        elif issubclass(param.annotation, aio_pika.Channel):
            values['channel_param_name'] = param.name

        elif issubclass(param.annotation, BaseModel):
            values['serializers'][param.name] = param.annotation

    return Listener.parse_obj(values)


class Pikantic:
    def __init__(self, *args, **kwargs):
        self._conn_args = args
        self._conn_kwargs = kwargs
        self._connection: Optional[Connection] = None
        self._listeners: List[Listener] = []

    def add_listener(self, queue_name: str, func: Callable[..., Any]):
        listener = create_rabbit_listener(queue_name, func)
        self._listeners.append(listener)

    def on_rabbit(self, queue_name: str):
        def decorator(func: Callable[..., Any]):
            if not inspect.iscoroutinefunction(func):
                raise ValueError(f"Callback '{func.__name__}' is not an async function.")

            self.add_listener(queue_name, func)

        return decorator

    async def async_run(self, loop):
        logging.basicConfig(level=logging.INFO)

        self._connection = await aio_pika.connect_robust(
            *self._conn_args,
            **self._conn_kwargs,
            loop=loop)
        async with self._connection:
            channel: Channel = await self._connection.channel()

            logging.info("Starting pikantic server.")

            for listener in self._listeners:
                asyncio.create_task(listener.listen(channel))

            await asyncio.Event().wait()

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_run(loop=loop))
