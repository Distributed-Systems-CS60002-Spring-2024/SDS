from utils import get_random_number

class ConsistantHash:
    slots: int
    k: int
    N: int
    consistant_hash: list[any]
    map: dict[str, int]
    shard_id: str

    def __init__(self, shard_id):
        self.slots = 512
        self.k = 9
        self.N = 3
        self.consistant_hash = [0] * self.slots
        self.map = {}
        self.shard_id = shard_id

    def h(self, i: int) -> int:
        return (i*i + 2*i + 17) % self.slots

    def fi(self, i: int,j: int) -> int:
        return (i*i + j*j + 2*j + 25) % self.slots

    def get_server_id(self, server: str) -> int:
        return self.map[server]

    # contains a list of server ids
    def build(self, server_list: set[str]):
        for server in server_list:
            self.add_server_to_hash(server)


    def get_server_from_request(self, request_id: int) -> str:
        req_pos = self.h(request_id)

        # move clockwise till you find the correct server
        for i in range(self.slots):
            if self.consistant_hash[req_pos] != 0:
                return self.consistant_hash[req_pos]
            else:
                req_pos = (req_pos + 1) % self.slots

        return None


    def add_server_to_hash(self, server: str):
        for j in range(self.k):
            # get the slot in the hash table and add the server to the map

            self.map[server] = get_random_number(6)
            pos = self.fi(self.get_server_id(server),j)
            if self.consistant_hash[pos] != 0:

                # use linear probing
                for x in range(self.slots):
                    pos = (pos + 1) % self.slots
                    if self.consistant_hash[pos] == 0:
                        self.consistant_hash[pos] = server
                        return True

                return False
            else:
                self.consistant_hash[pos] = server
                return True


    def remove_server_from_hash(self, server: str):
        for i in range(self.slots):
            if self.consistant_hash[i] != 0:
                if self.get_server_id(server) == self.get_server_id(self.consistant_hash[i]):
                    self.consistant_hash[i] = 0
                    del(self.map[server])