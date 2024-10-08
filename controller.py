from enum import Enum, auto
import zmq  
import dataclasses
from typing import List, Optional, Union
from fastapi import FastAPI, File, Form, Request, UploadFile


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
        self.load_balance_method = LoadBalanceMethod.from_str(server_args.load_balance_method)
        
        context = zmq.contenxt()
        self.recv_from_nodes = context.socket(zmq.PULL)
    
    
    # def recv_register_nodes(self):