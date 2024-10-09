from typing import Optional

from pydantic import BaseModel


class ModelTrainSchema(BaseModel):
    account: str
    connstr: str
    container: Optional[str] = None
    folderpath: Optional[str] = None
    modelName: Optional[str] = None
