from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from app.validation import *
from .database import Base,engine
_type_lookup = {"Number":Integer,"String":String}

class ShardT(Base):
    __tablename__="shardt"
    Stud_id_low=Column(Integer,primary_key=True)
    Shard_id=Column(Integer)
    Shard_size=Column(Integer)
    valid_idx=Column(Integer)
class MapT(Base):
    __tablename__="mapt"
    _id=Column(Integer,primary_key=True,autoincrement=True)
    server_id=Column(Integer)
    Shard_id=Column(Integer)

