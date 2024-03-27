# SDS

A Scalable Database with Sharding - CS60002 Distributed Systems

## Running Docker Containers

To initiate the deployment of load balancer containers, execute the following command:
```
make run
```
This command will launch the load balancer container, which, in turn, will spawn the initial N server Docker containers along with their heartbeat probing threads. Ensure that the necessary configurations are in place for a seamless deployment. The command also clears any existing containers using server or load balancer image (i.e. execute make clean).

## Remove Existing Container

To stop and remove all containers using the server image and load balancer, run the following command:
```
make clean
```
Executing this command is recommended before running the main code to ensure there are no conflicting container instances already running. This helps prevent potential errors and ensures a clean environment for the code execution.

## Design Choices

1. Every server container maintains both a SQL database to store the shard data and a server application to handle config/read/write/delete operations. So, the server container is a combination of a SQL database and a server application. It handles the requests/queries from the load balancer and updates the data in the SQL database accordingly, as well as sends the response back to the load balancer.
2. When executing the /add endpoint, users may provide existing server hostnames as part of the request. Users can specify a load balancer to generate a random id for the new server using server[0] type of syntax.
3. In /rm endpoint, the load balancer will make sure that the specified number of servers are removed, if lesser number of servers are specified, then the load balancer will randomly select other servers to fulfil the request.
4. The load balancer has a deamon thread, which sends the headtbeat request to the server every 30 seconds, if  a reply is not received, the server spawns a new server container and makes sure the shards it previously had are again copied back to the new container.

## Evaluation

### Pre-Analysis Setup

  initialise and deploy containers:
   ```
   make install
   make run
   ```
### Part-1: Read and Write Speed Analysis
  initialise database container with default configuration:
```
   cd analysis/
   python test1.py --type init
   python test1.py --type status

  run analysis file
   python test1.py --type write --nreqs 10000        
   python test1.py --type read --nreqs 1000
```
Leveraging the default configuration, i.e.,
```
  NUM_SERVERS: 6
  NUM_SHARDS: 4
  NUM_REPLICAS: 3
```
We obtain the following statistics for 10000 write and read requests respectively:
```
  - Request Type: write

      No of successful requests: 10000/10000
      No of failed requests: 0/10000
      Time taken to send 10000 requests: 188.25956535339355 seconds

  - Request Type: read

      No of successful requests: 10000/10000
      No of failed requests: 0/10000
      Time taken to send 10000 requests: 60.557310581207275 seconds
```
### Part-2: Scaling number of shard replicas to 6
```
   initialise database container with specific configuration
   cd db_analysis/
   python test2.py --type init         
   python test2.py --type status       

   run analysis file
   python test2.py --type write --nreqs 10000        
   python test2.py --type read --nreqs 10000
```

On setting NUM_REPLICAS=6, keeping the number of servers and shards fixed, i.e.,
```
  NUM_SERVERS: 6
  NUM_SHARDS: 4
  NUM_REPLICAS: 6
```
We obtain the following statistics for 10000 write and read requests respectively:
```
- Request Type: write

      No of successful requests: 10000/10000
      No of failed requests: 0/10000
      Time taken to send 10000 requests: 571.5665924549103 seconds

- Request Type: read

      No of successful requests: 9995/10000
      No of failed requests: 5/10000
      Time taken to send 10000 requests: 109.68647050857544 seconds
```
The increased latency for write and read requests can be attributed to the increased number of replicas for each shard. This implies that both write and read requests need to access all replicas of a shard to maintain consistency, increasing the time taken to handle requests.
### Part-3 : Scaling number of servers to 10 and number of replicas to 8
```
   initialise database container with specific configuration
   cd db_analysis/
   python test3.py --type init        
   python test3.py --type status       

   run analysis file
   python test3.py --type write --nreqs 10000        
   python test3.py --type read --nreqs 10000
   ```
The following configuration for the database server, i.e.,
```
  NUM_SERVERS: 10
  NUM_SHARDS: 6
  NUM_REPLICAS: 8
```
yields the following statistics for 10000 write and read requests respectively:
```
- Request Type: write

  No of successful requests: 10000/10000
  No of failed requests: 0/10000
  Time taken to send 10000 requests: 758.3099572658539 seconds

- Request Type: read

  No of successful requests: 9999/10000
  No of failed requests: 1/10000
  Time taken to send 10000 requests: 110.17270064353943 seconds
```
In this case, there is a noticeable, albeit slight increase in the latency for write and read requests compared to Part-2.

Why isn't the increase in latency for read requests as prominent as Part-2? It has to do with the fact that an increase in the number of servers leads to better distribution of read requests, implying that incoming requests face lesser contention while accessing shard replicas across a large number of servers. This leads to only a slight increase in the latency for handling read requests, as shown above.

For write requests, an increase in the number of replicas to be edited overcomes the benefit of less contention due to more servers, leading to a marked increase in latency for processing write requests.

### Part-4: Endpoint Checking and Server Drop Analysis
#### Endpoint Checking
```
   initialise database container with specific configuration:
   cd db_analysis/
   python test4.py --type init         # initialise database
   python test4.py --type status       # check status

   write/read requests:
   python test4.py --type write --nreqs 100        
   python test4.py --type read --nreqs 100

   update/delete requests:
   python test4.py --type update       # updates a random db entry
   python test4.py --type delete       # deletes a random entry from all involved replicas

   add/remove servers:
   python test4.py --type add        # adds list of servers mentioned in script
   python test4.py --type rm         # removes list of servers mentioned in script
```

