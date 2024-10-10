from enum import Enum, auto
import zmq  

from typing import List, Optional, Union
from fastapi import FastAPI, File, Form, Request, UploadFile
from io_struct import NodeInfo

from fastapi.responses import StreamingResponse

import aiohttp
import logging

from multiprocessing import Process

logger = logging.getLogger(__name__)
AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(total=6 * 60 * 60)


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
        self.recv_controller_procs = []
        
        
        self.round_robin_counter = 0
        dispatch_lookup = {
            LoadBalanceMethod.ROUND_ROBIN: self.round_robin_scheduler
        }
        
        self.dispatching = dispatch_lookup[self.load_balance_method]
        
        self.context = zmq.Context(2)
        
        
    def add_new_node(self, nodeInfo:NodeInfo):
        logger.info(f'{nodeInfo.ip}:{nodeInfo.port} is registered on server..')
        self.node_list.append(nodeInfo)
        if nodeInfo.controller_info_port is not None:
            recv_controller_info = self.context.socket(zmq.PULL)
            recv_controller_info.connect(f'tcp://{nodeInfo.ip}:{nodeInfo.controller_info_port}')
            
            # start a new process, call the function recv_controller_info_loop
            proc = Process(target=self.recv_controller_info_loop, args=(recv_controller_info,))
            proc.start()
            self.recv_controller_procs.append(proc)
            
    def recv_controller_info_loop(self, recv_controller_info):
        while True:
            try:
                controller_info = recv_controller_info.recv_string(zmq.NOBLOCK)
                logger.info(controller_info)
            except zmq.Again:
                pass
        
    # TODO change it to send requests to nodes.
    async def round_robin_scheduler(self, input_requests, base_url):
        async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT) as session:
            for req in input_requests:
                pay_load = req.dict()
                target_node = self.node_list[self.round_robin_counter]
                self.round_robin_counter = (self.round_robin_counter + 1) % len(self.node_list)
                url = f'http://{target_node.ip}:{target_node.port}/{base_url}'
                async with session.post(url, json=pay_load) as response:
                    if response.status == 200:
                        # 使用异步生成器产生数据块
                        async for chunk in response.content:
                            yield chunk
                    else:
                        print("Failed to retrieve data:", response.status)
                        yield b''  # 返回空字节，表示错误或无数据


    def __del__(self):
        for proc in self.recv_controller_procs:
            proc.terminate()
            proc.join()
        
        self.context.term()
        