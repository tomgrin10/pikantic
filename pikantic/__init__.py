import asyncio
import functools
from typing import Callable, Any, Optional
import fastapi

import aio_pika
from aio_pika import Connection

fastapi.FastAPI.get()


class Listener:
    pass


def create_rabbit_listener(func: Callable[..., Any])



class Pikantic:
    def __init__(self, *args, **kwargs):
        self._conn_args = args
        self._conn_kwargs = kwargs
        self._connection: Optional[Connection] = None

    def add_listener(self, func: Callable[..., Any]):
        pass

    def on_rabbit(self, queue_name: str):
        def decorator(func: Callable[..., Any]):
            self.add_listener(func)

        return decorator

    async def async_run(self):
        self._connection = await aio_pika.connect_robust(*self._conn_args,
                                                         **self._conn_kwargs)
        with self._connection:
            channel: aio_pika.Channel = await self._connection.channel()

            for listener in self._listeners:
                asyncio.create_task(listener.listen())

    def run(self):
        asyncio.run(self.async_run())
