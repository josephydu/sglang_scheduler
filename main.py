
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse, Response, StreamingResponse
import uvicorn
import uvloop
import argparse
from server_args import ServerArgs

from fastapi.middleware.cors import CORSMiddleware

from controller import Controller
from io_struct import NodeInfo

app = FastAPI()

controller = None


@app.post("/register_nodes")
async def register_nodes(nodeInfo: NodeInfo):
    """Register nodes to the controller."""
    print(nodeInfo)
    return JSONResponse({"message": "Register nodes SUCCESS to the controller."})

@app.post("/handle_request")
async def handle_request(req: Request):
    if controller is not None:
        controller.dispatching(req)
    
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
    
    controller = Controller(server_args=server_args)
    
    launch_server(server_args)