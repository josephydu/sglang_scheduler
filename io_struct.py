from pydantic import BaseModel

class NodeInfo(BaseModel):
    ip: str
    port: int
    # gpu_id: int