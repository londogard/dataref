from pydantic import BaseModel


class Table(BaseModel):
    name: str
    schema: str
    description: str
    columns: list["Column"]
    tags: list["Tag"]


class Column(BaseModel):
    name: str
    type: str
    description: str
    tags: list


class Tag(BaseModel):
    name: str
    description: str
