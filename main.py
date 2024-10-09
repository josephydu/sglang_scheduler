
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse, Response, StreamingResponse
import uvicorn
import uvloop
import argparse
from server_args import ServerArgs

from fastapi.middleware.cors import CORSMiddleware

from controller import Controller
from io_struct import NodeInfo
import logging

import asyncio

import requests

app = FastAPI()

controller = None

async def data_stream(controller, input_requests, base_url):
    async for data_chunk in controller.dispatching(input_requests, base_url):
        if data_chunk:
            yield data_chunk
        else:
            yield b''

@app.post("/register_nodes")
async def register_nodes(nodeInfo: NodeInfo):
    """Register nodes to the controller."""
    if controller is not None:
        controller.add_new_node(nodeInfo=nodeInfo)
    return JSONResponse({"message": "Register nodes SUCCESS to the controller."})

@app.post("/generate")
async def handle_request(req: Request):
    if controller is not None:
        base_url = "generate"
        return await controller.dispatching([req], base_url)
    else:
        return None
    
@app.post("/v1/completions")
async def openai_v1_completions(req: Request):
    if controller is not None:
        base_url = "v1/completions"
        return StreamingResponse(data_stream(controller=controller, input_requests=[req], base_url=base_url))

@app.get("/get_model_info")
async def get_model_info():
    if controller is not None:
        if len(controller.node_list) != 0:
            return {
                "model_path": controller.node_list[0].model_path,
                "is_generation": controller.node_list[0].is_generation
            }


def launch_server(server_args):
    uvicorn.run(
            app,
            host=server_args.host,
            port=server_args.port,
            log_level=server_args.log_level_http or server_args.log_level,
            timeout_keep_alive=5,
            loop="uvloop",
        )
        
    
if __name__ == "__main__":
    parser_args = argparse.ArgumentParser()
    ServerArgs.add_cli_args(parser_args)
    args = parser_args.parse_args()
    server_args = ServerArgs.from_cli_args(args)
    print(server_args)
    
    
    logging.basicConfig(
        level=getattr(logging, server_args.log_level.upper()),
        format="%(message)s",
    )

    
    controller = Controller(server_args=server_args)
    
    launch_server(server_args)