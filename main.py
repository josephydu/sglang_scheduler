
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse, Response, StreamingResponse
import uvicorn
import uvloop
import argparse
from server_args import ServerArgs
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有源
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有头
)


@app.post("/register_nodes")
def register_nodes(request: Request):
    """Register nodes to the controller."""
    print(request.json())
    return JSONResponse({"message": "Register nodes SUCCESS to the controller."})

    
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
    launch_server(server_args)