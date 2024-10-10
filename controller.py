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
            LoadBalanceMethod.ROUND_ROBIN: self.round_robin_scheduler,
            LoadBalanceMethod.POWER_OF_2_CHOICE: self.power_of_2_choice_scheduler,
            LoadBalanceMethod.RESOURCES_AWARE: self.resources_aware_scheduler,
        }
        
        self.dispatching = dispatch_lookup[self.load_balance_method]
        
        self.context = zmq.Context(2)
        
        self.log_step = 10
        
        
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
                recv_controller_info_str = recv_controller_info.recv_string(zmq.NOBLOCK)
                if recv_controller_info_str is not None and recv_controller_info_str != "":
                    logger.info(f'[recv_controller_info_loop]{recv_controller_info_str}')
                    ip, port, avaliable_memory, num_running, num_waitting = recv_controller_info_str.split(",")
                    self.controller_info_dict[f'{ip}:{port}'] = (int(avaliable_memory), int(num_running), int(num_waitting))
            except zmq.Again:
                pass
        
    #change it to send requests to nodes.
    async def round_robin_scheduler(self, input_requests, base_url):
        if input_requests == 0 or self.node_list == 0:
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

    async def power_of_2_choice_scheduler(self, input_requests, base_url):
        if input_requests == 0 or self.node_list == 0 or len(self.controller_info_dict) == 0:
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

    async def resources_aware_scheduler(self, input_requests, base_url):
        if input_requests == 0 or self.node_list == 0 or len(self.controller_info_dict) == 0:
            return 
        async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT) as session:
            for req in input_requests:
                # power of 2 choice 
                if len(self.controller_info_dict) == 1:
                    target_node, infos = next(iter(self.controller_info_dict.items()))
                else:
                    available_mem = []
                    num_reqs_running = []
                    num_reqs_waiting = []
                    nodes_list = []
                    for key, value in self.controller_info_dict.items():
                        nodes_list.append(key)
                        available_mem.append(value[0])
                        num_reqs_running.append(value[1])
                        num_reqs_waiting.append(value[2])

                    # 判断是否是全部waiting
                    all_waitting = False
                    if min(num_reqs_waiting) > 0:
                        # 最小值都大于0，全部waiting
                        all_waitting = True
                    else:
                        # 最小值都是0， 则全部waiting
                        all_waitting = False
                    # 选出不waiting
                    no_waiting = [1 if waiting == 0 else 0 for waiting in num_reqs_waiting]
                    if all_waitting:
                        # 全部waiting，选最小的
                        ratio = [
                            run / wait for run, wait in zip(num_reqs_running, num_reqs_waiting)
                        ]
                        # run越大 认为后续释放的可能性越多，wait越少，说明后续计算能力更强
                        min_value = max(ratio)
                        # 找到所有最小值的索引
                        min_indices = [i for i, x in enumerate(ratio) if x == min_value]
                        # 从这些索引中随机选择一个
                        index = random.choice(min_indices)
                        # 从waitting最小的找到available最大的
                    else:
                        # 选出不waiting的且available mem最大的
                        # no_waiting 和available做乘法，找最大

                        filter_result = [a * b for a, b in zip(no_waiting, available_mem)]
                        index = filter_result.index(max(filter_result))
                    target_node = nodes_list[index]

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
        
        # self.context.term()
        