"""
Copyright 2023-2024 SGLang Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

"""The arguments of the server."""

import argparse
import dataclasses
import random
from typing import List, Optional, Union


@dataclasses.dataclass
class ServerArgs:
    # Port
    host: str = "127.0.0.1"
    port: int = 8080
    
    # Logging
    log_level: str = "info"
    log_level_http: Optional[str] = None
    log_requests: bool = False
    show_time_cost: bool = False

    # Random seed
    random_seed: int = random.randint(0, 1000000000)
    
    # Other
    load_balance_method: str = "round_robin"

    @staticmethod
    def add_cli_args(parser: argparse.ArgumentParser):
        parser.add_argument(
            "--host", type=str, default=ServerArgs.host, help="The host of the server."
        )
        parser.add_argument(
            "--port", type=int, default=ServerArgs.port, help="The port of the server."
        )
        parser.add_argument(
            "--random-seed",
            type=int,
            default=ServerArgs.random_seed,
            help="The random seed.",
        )
        parser.add_argument(
            "--log-level",
            type=str,
            default=ServerArgs.log_level,
            help="The logging level of all loggers.",
        )
        parser.add_argument(
            "--log-level-http",
            type=str,
            default=ServerArgs.log_level_http,
            help="The logging level of HTTP server. If not set, reuse --log-level by default.",
        )
        parser.add_argument(
            "--log-requests",
            action="store_true",
            help="Log the inputs and outputs of all requests.",
        )
        parser.add_argument(
            "--show-time-cost",
            action="store_true",
            help="Show time cost of custom marks.",
        )
        parser.add_argument(
            "--load-balance-method",
            type=str,
            default=ServerArgs.load_balance_method,
            help="The load balancing strategy for data parallelism.",
            choices=[
                "round_robin",
                "shortest_queue",
                "resources_aware",
                "power_of_2_choice",
                "pre_radix",
                "multi_turn",
                "bucket",
            ],
        )

    @classmethod
    def from_cli_args(cls, args: argparse.Namespace):
        attrs = [attr.name for attr in dataclasses.fields(cls)]
        return cls(**{attr: getattr(args, attr) for attr in attrs})

    def url(self):
        return f"http://{self.host}:{self.port}"
