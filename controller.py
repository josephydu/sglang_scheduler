from enum import Enum, auto
import zmq  

import dataclasses
from typing import List, Optional, Union
from fastapi import FastAPI, File, Form, Request, UploadFile
from io_struct import NodeInfo

from fastapi.responses import StreamingResponse

import aiohttp
import logging

import requests

logger = logging.getLogger(__name__)
AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(total=6 * 60 * 60)
@dataclasses.dataclass
class PortArgs:
    tokenizer_port: int
    controller_port: int
    detokenizer_port: int
    nccl_ports: List[int]
class LoadBalanceMethod(Enum):
    """Load balance method."""

    ROUND_ROBIN = auto()
    SHORTEST_QUEUE = auto()
    RESOURCES_AWARE = auto()
    POWER_OF_2_CHOICE = auto()
    PRE_RADIX = auto()
    MULTI_TURN = auto()
    BUCKET = auto()
    
    @classmethod
    def from_str(cls, method: str) -> Enum:
        """Return the enum value from the string."""
        method = method.upper()
        try:
            return cls[method]
        except KeyError as exc:
            raise ValueError(f'{method} is not a valid load balance method') from exc


class Controller:
    def __init__(self, server_args) -> None:
        self.server_args = server_args
        self.load_balance_method = LoadBalanceMethod.from_str(server_args.load_balance_method)

        self.node_list = []
        
        
        self.round_robin_counter = 0
        dispatch_lookup = {
            LoadBalanceMethod.ROUND_ROBIN: self.round_robin_scheduler
        }
        
        self.dispatching = dispatch_lookup[self.load_balance_method]
        
        
    def add_new_node(self, nodeInfo:NodeInfo):
        logger.info(f'{nodeInfo.ip}:{nodeInfo.port} is registered on server..')
        self.node_list.append(nodeInfo)
    
    
    # TODO change it to send requests to nodes.
    async def round_robin_scheduler(self, input_requests, base_url):
        # pass
        # logger.info(input_requests)
        # logger.info(await input_requests[0].json())
        # if len(input_requests) == 0 or len(self.node_list) == 0:
            # return
        # async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT) as session:
        for req in input_requests:
            pay_load = await req.json()
            target_node = self.node_list[self.round_robin_counter]
            self.round_robin_counter = (self.round_robin_counter + 1) % len(self.node_list)
            url=f'http://{target_node.ip}:{target_node.port}/{base_url}'
            print(requests.post(url=url, json=pay_load, stream=True).content)
            # async with session.post(
                # url=f'http://{target_node.ip}:{target_node.port}/{base_url}',
                # json=pay_load) as res:
                # if res.status == 200:
                    # async def stream_response():
                        # async for data in res.content.iter_any():
                            # yield data
                    # return StreamingResponse(stream_response(), media_type="application/json")
                # else:
                    # return {"status": res.status, "message": await res.text()}
                # # 处理响应
                # # logger.info(f"{response}, {response.content}")
                # # return response.json()
                # return None
