from enum import Enum, auto
import zmq  

from typing import List, Optional, Union
from fastapi import FastAPI, File, Form, Request, UploadFile
from io_struct import NodeInfo

from fastapi.responses import StreamingResponse

import aiohttp
import logging

import threading
import random
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
        self.controller_info_dict = {}# key is "ip:port", value is "(available memory, running, waiting)"
        
        
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
            proc = threading.Thread(target=self.recv_controller_info_loop, args=(recv_controller_info,))
            proc.start()
            self.recv_controller_procs.append(proc)
            
    def recv_controller_info_loop(self, recv_controller_info):
        while True:
            try:
                recv_controller_info = recv_controller_info.recv_string(zmq.NOBLOCK)
                # logger.info(controller_info)
                if recv_controller_info is not None and recv_controller_info != "":
                    ip, port, avaliable_memory, num_running, num_waitting = recv_controller_info.split(",")
                    self.controller_info_dict[f'{ip}:{port}'] = (avaliable_memory, num_running, num_waitting)
            except zmq.Again:
                pass
        
    #change it to send requests to nodes.
    async def round_robin_scheduler(self, input_requests, base_url):
        if self.input_requests == 0 or self.node_list == 0:
            return 
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

    async def power_of_2_choice(self, input_requests, base_url):
        if self.input_requests == 0 or self.node_list == 0 or len(self.controller_info_dict) == 0:
            return 
        async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT) as session:
            for req in input_requests:
                # power of 2 choice 
                if len(self.controller_info_dict) == 1:
                    target_node, infos = next(iter(self.controller_info_dict.items()))
                else:
                    # random choose 2 nodes
                    (k1, v1), (k2, v2) = random.sample(list(self.controller_info_dict.items()), 2)
                    
                    # 1. compare the waiting
                    if v1[2] != v2[2]:
                        target_node = k1 if v1[2] < v2[2] else k2
                    elif v1[1] != v2[1]:
                        target_node = k1 if v1[1] < v2[1] else k2
                    elif v1[0] != v2[0]:
                        target_node = k1 if v1[0] > v2[0] else k2
                    else:
                        target_node = k1
                logger.info(f"choosen nodes in power of 2 choice is [{target_node}]")
                url = f'http://{target_node}/{base_url}'
                
                
                pay_load = req.dict()
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
        