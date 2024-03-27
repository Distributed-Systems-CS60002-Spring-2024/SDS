@lb.post("/read")
async def read_student_data(request : Request):
    payload = await request.json()
    Stud_id = payload['Stud_id']
    low = Stud_id["low"]
    high = Stud_id["high"]

    res = engine.execute("SELECT Shard_id FROM ShardT WHERE Stud_id_low <= %s AND Stud_id_low + Shard_size > %s".format(high,low))
    shard_list = res.fetchall()

    data = {}
    for shard in shard_list:
        # get the server corresponding to this shard
        server : int
        for entry in ShardHashMaps:
            if entry.shard_id == shard:

                # LOCKKKKK
                lock_to_hashmap[shard].acquire()
                server = entry.get_server_from_request(random.randint(0,99999))
                lock_to_hashmap[shard].release()

        # request the server for the data
        if server != None:
            try:
                reply = requests.post(f'http://{server}:{serverport}/read', json = {
                    "shard": shard,
                    "Stud_id": {"low": low, "high": high}
                })
                data[shard] = reply.json()
            except requests.exceptions.ConnectionError:
                message = '<ERROR> Server unavailable'
                data[shard] = {'message': message, 'status': 'failure'}
        else:
            message = '<ERROR> Server unavailable'
            data[shard] = {'message': message, 'status': 'failure'}

    final_data = []
    for shard in data:
        if data[shard]['status'] == 'success':
            final_data += data[shard]['data']

    return_dict = {}
    return_dict.update({"shards_queried" : shard_list, "data" : final_data, "status" : "success"})
    return return_dict, 200



@lb.put("/update")
async def update_entry(request : Request):
    payload = await request.json()
    Stud_id = payload["Stud_id"]
    data = payload["data"]

    res = engine.execute("SELECT Shard_id FROM ShardT WHERE Stud_id_low <= %s AND Stud_id_low + Shard_size > %s".format(Stud_id, Stud_id))
    shard = res.fetchone()

    # find the servers corresponding to this shard
    res = engine.execute("SELECT Server_id FROM mapt WHERE Shard_id = %s".format(shard))
    servers = res.fetchall()

    if len(servers) == 0:
        message = '<ERROR> No servers available for the shard'
        return {'message': message, 'status': 'failure'}, 400
    
    # update the entry in all servers

    lock_to_hashmap[shard].acquire()
    for server in servers:
        server = "Server" + str(server)
        try:
            reply = requests.put(f'http://{server}:8000/update', json = {
                "shard": shard,
                "Stud_id": Stud_id,
                "data": data
            })
            return reply.json(), reply.status_code
        except requests.exceptions.ConnectionError:
            message = '<ERROR> Server unavailable'
            return {'message': message, 'status': 'failure'}, 400
        
    lock_to_hashmap[shard].release()
    
    message = "Data entry for Stud_id:%s updated".format(Stud_id)


    return {'message': message, 'status':'success'}, 200
    

@lb.post("/write")
async def write_to_server(request : Request):
    payload = await request.json()
    data_to_write = payload["data"]

    # sort the data as per student id
    sorted_data = sorted(data_to_write, key=lambda x: x['Stud_id'])

    res = engine.execute("SELECT * FROM SHARDT")
    shard_data = res.fetchall()

    for shard in shard_data:
        data_to_insert = []
        while data_idx < len(sorted_data) and sorted_data[data_idx]['Stud_id'] < shard[0] + shard[2]:
            data_to_insert.append(sorted_data[data_idx])
            data_idx += 1
        
        if len(data_to_insert) == 0:
            continue
    
        
        # query all the servers having the shard
        res = engine.execute(f"SELECT Server_id FROM MapT WHERE Shard_id = {shard[1]}")
        servers = res.fetchall()

        # if no servers are available, return error
        if len(servers) == 0:
            message = '<ERROR> No servers available for the shard'
            return {'message': message, 'status': 'failure'}, 400
        
        lock_to_hashmap[shard[1]].acquire()
        for server in servers:
            try:

                shard_idx = shard[3]

                reply = requests.post(f'http://{server}:8000/write', json = {
                    "shard": shard[1],
                    "curr_idx": shard_idx,
                    "data": data_to_insert
                })

                if reply.status_code == 200:
                    newShardIndex = reply.json()['current_idx']

                
            except requests.exceptions.ConnectionError:
                message = '<ERROR> Server unavailable'
                return {'message': message, 'status': 'failure'}, 400
            
        # update the valid index of the shard
        session = SessionLocal()
        engine.execute(f"UPDATE ShardT SET valid_idx = {newShardIndex} WHERE Shard_id = {shard[1]}")

        session.commit()
        lock_to_hashmap[shard[1]].release()

    response ={
        "message": f"{len(sorted_data)} Data entries added",
        "status": "success"
    }

    return jsonify(response), 200


@lb.delete("/del")
async def delete(request : Request):
    payload = await request.json()
    Stud_id = payload["Stud_id"]

    res = engine.execute("SELECT Shard_id FROM ShardT WHERE Stud_id_low <= %s AND Stud_id_low + Shard_size > %s".format(Stud_id, Stud_id))
    shard = res.fetchone()

    # find the servers corresponding to this shard
    res = engine.execute("SELECT Server_id FROM mapt WHERE Shard_id = %s".format(shard))
    servers = res.fetchall()

    if len(servers) == 0:
        message = '<ERROR> No servers available for the shard'
        return {'message': message, 'status': 'failure'}, 400
    
    # update the entry in all servers
    lock_to_hashmap[shard].acquire()
    for server in servers:
        server = "Server" + str(server)
        try:
            reply = requests.delete(f'http://{server}:8000/del', json = {
                "shard": shard,
                "Stud_id": Stud_id,
            })
            return reply.json(), reply.status_code
        except requests.exceptions.ConnectionError:
            message = '<ERROR> Server unavailable'
            return {'message': message, 'status': 'failure'}, 400
        
    lock_to_hashmap[shard].release()
    
    message = "Data entry for Stud_id:%s deleted".format(Stud_id)


    return {'message': message, 'status':'success'}, 200
