from enum import Enum
from typing import Annotated
from fastapi import Body,FastAPI,Response, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
# from .models import create_schema
from app.models import create_schema
from app import crud
from app.validation import *
from app.database import SessionLocal
SERVER_ID=0
app = FastAPI()
def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close
@app.get("/")
async def root():
    return {"message:":"Hello from server"}

@app.post("/config")
async def config(schema:Schema,shards:Annotated[set[str],Body()])->Config_response:
    msg=""
    for shard in shards:
        table=create_schema(schema,shard)
        print(table)
        msg+=f"Server{SERVER_ID}:{shard}, "
    msg+="configured"
    res=Config_response()
    res.message=msg
    res.status="success"
    return res

@app.get("/heartbeat")
async def get_heartbeat():
    return {}

@app.post("/copy")
async def copy(shards:copy_payload):
    db=get_db()
    data=dict()
    for shard in shards.shards:
        data.update({shard:crud.read(db,shard)})
    data.update({"status":"success"})
    return data

@app.post("/read")
async def read(body:read_payload):
    db=get_db()
    data=crud.read(db,body.shard,body.Stud_id.low,body.Stud_id.high)
    response=dict()
    response.update({"data":data})
    response.update({"status":"success"})
    return response

@app.post("/write")
async def write(body:write_payload,response:Response):
    db=get_db()
    ret=crud.write(db,body.shard,body.data)
    res=dict()
    if ret!=-1:
        res.update({"message":f"student with Stud_id:{ret} already exists"})
        res.update({"status":"failure"})
        response.status_code=400
    else:
        res.update({"message":"Data entries added"})
        res.update({"current_idx":body.curr_idx+len(body.data)})
        res.update({"status":"success"})
        response.status_code=200
    return res

@app.put("/update")
async def update(body:update_payload,response:Response):
    db=get_db()
    result=crud.update(db,body.shard,body.Stud_id,body.data)
    res=dict()
    if(result==1):
        res.update({"message":f"Data entry for Stud_id:{body.Stud_id} updated"})
        res.update({"status":"success"})
        response.status_code=200
    else:
        res.update({"message":f"Stud_id:{body.Stud_id} doesn't exist"})
        res.update({"status":"failure"})
        response.status_code=404

    return res

@app.delete("/del")
async def delete(body:del_payload,response:Response):
    db=get_db()
    result=crud.delete(db,body.shard,body.Stud_id)
    res=dict()
    if(result==1):
        res.update({"message":f"Data entry for Stud_id:{body.Stud_id} deleted"})
        res.update({"status":"success"})
        response.status_code=200
    else:
        res.update({"message":f"Stud_id:{body.Stud_id} doesn't exist"})
        res.update({"status":"failure"})
        response.status_code=404
    return res




