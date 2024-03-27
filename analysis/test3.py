import random
import time
import asyncio
import argparse
from utils import send_requests

random.seed(42)

'''
    TASK 3:

     - Increase servers to 10, shards to 6 and replicas to 8.
     - Compute speeds for 10000 read and 10000 write requests.

'''

SHARD_SIZE = 4096
NUM_SERVERS = 10
NUM_SHARDS = 6
NUM_REPLICAS = 8

parser = argparse.ArgumentParser()

parser.add_argument("--type", type=str, required=True, default="read")
parser.add_argument("--nreqs", type=int, default=1)

args = parser.parse_args()

if __name__ == '__main__':
    
    request_type = args.type
    num_requests = args.nreqs

    try:   
            
        print(f"Request Type: {request_type}")
         
        if request_type == "init":
            start = time.time()
            config =  {
                "N": NUM_SERVERS,
                "schema": {
                            "columns":["Stud_id","Stud_name","Stud_marks"],
                            "dtypes":["Number","String","String"]
                        },
                "shards": [
                    {
                        "Stud_id_low": SHARD_SIZE * i,
                        "Shard_id": f"sh{i+1}",
                        "Shard_size": SHARD_SIZE
                    }
                    for i in range(NUM_SHARDS)
                ],
                "servers":
                { 
                            'Server0': ['sh1', 'sh2', 'sh3', 'sh4', 'sh5'],
                            'Server1': ['sh1', 'sh2', 'sh3', 'sh4', 'sh6'],
                            'Server2': ['sh1', 'sh2', 'sh3', 'sh5', 'sh6'],
                            'Server3': ['sh1', 'sh2', 'sh4', 'sh5', 'sh6'],
                            'Server4': ['sh1', 'sh3', 'sh4', 'sh5', 'sh6'],
                            'Server5': ['sh2', 'sh3', 'sh4', 'sh5', 'sh6'],
                            'Server6': ['sh2', 'sh3', 'sh4', 'sh5', 'sh1'],
                            'Server7': ['sh3', 'sh4', 'sh5', 'sh6', 'sh2'],
                            'Server8': ['sh1', 'sh3', 'sh5', 'sh6'],
                            'Server9': ['sh2', 'sh4', 'sh1', 'sh6']
                            
                        }
            }
            asyncio.run(send_requests(1,"init",config))
            end = time.time()
            print(f"Time taken to send init request: {end-start} seconds")

        if request_type == "status":
            start = time.time()
            asyncio.run(send_requests(1,"status"))
            end = time.time()
            print(f"Time taken to send status request: {end-start} seconds")
            
        elif request_type == "write":    
            start = time.time()
            asyncio.run(send_requests(num_requests,"write"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")

        elif request_type == "read":
            start = time.time()
            asyncio.run(send_requests(num_requests,"read"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")
        
        elif request_type == "update":
            start = time.time()
            asyncio.run(send_requests(num_requests,"update"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")
            
        elif request_type == "delete":
            start = time.time()
            asyncio.run(send_requests(num_requests,"delete"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")
        
    except Exception as e:
        print(f"Error: An exception occurred in overall run: {e}")
