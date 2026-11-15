
from pydantic import BaseModel, Field
from datetime import datetime

class WaitTaskInput(BaseModel):
    seconds: float = Field(gt=0)


class Job(BaseModel):
    id : int
    task_name : str
    status : str
    priority : int
    payload : str
    created_at : datetime