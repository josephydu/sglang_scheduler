from pydantic import BaseModel

class NodeInfo(BaseModel):
    ip: str
    port: int
    model_path: str
    is_generation: bool
    # gpu_id: int