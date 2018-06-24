from .common import *

class NameNode(threading.Thread):
    """
    Name Server，handle instructions and manage data servers
    Client can use `ls, read, fetch` cmds.
    """
    def __init__(self, name, gconf):
        super(NameNode, self).__init__(name=name)
        self.gconf = gconf # global parameters
        self.metas = None
        self.id_chunk_map = None # file id -> chunk, eg. {0: ['0-part-0'], 1: ['1-part-0']}
        self.id_file_map = None # file id -> name, eg. {0: ('README.md', 1395), 1: ('mini_dfs.py', 14603)}
        self.chunk_server_map = None # chunk -> data servers, eg. {'0-part-0': [0, 1, 2], '1-part-0': [0, 1, 2]}
        self.last_file_id = -1 # eg. 1
        self.last_data_server_id = -1 # eg. 2
        self.load_meta()

    def run(self):
        gconf = self.gconf
        while True:
            # waiting for cmds
            gconf.name_event.wait()

            if gconf.cmd_flag:
                if gconf.cmd_type == COMMAND.put:
                    self.put()
                elif gconf.cmd_type == COMMAND.read:
                    self.read()
                elif gconf.cmd_type == COMMAND.fetch:
                    self.fetch()
                elif gconf.cmd_type == COMMAND.ls:
                    self.ls()
                else:
                    pass
            gconf.name_event.clear()

    def load_meta(self):
        """load Name Node Meta Data"""

        if not os.path.isfile(NAME_NODE_META_PATH):
            self.metas = {
                'id_chunk_map': {},
                'id_file_map': {},
                'chunk_server_map': {},
                'last_file_id': -1,
                'last_data_server_id': -1
            }
        else:
            with open(NAME_NODE_META_PATH, 'rb') as f:
                self.metas = pickle.load(f)
        self.id_chunk_map = self.metas['id_chunk_map']
        self.id_file_map = self.metas['id_file_map']
        self.chunk_server_map = self.metas['chunk_server_map']
        self.last_file_id = self.metas['last_file_id']
        self.last_data_server_id = self.metas['last_data_server_id']

    def update_meta(self):
        """update Name Node Meta Data after put"""

        with open(NAME_NODE_META_PATH, 'wb') as f:
            self.metas['last_file_id'] = self.last_file_id
            self.metas['last_data_server_id'] = self.last_data_server_id
            pickle.dump(self.metas, f)

    def ls(self):
        """ls print meta data info"""

        print('total', len(self.id_file_map))
        for file_id, (file_name, file_len) in self.id_file_map.items():
            print(LS_PATTERN % (file_id, file_name, file_len))
        self.gconf.ls_event.set()

    def put(self):
        """split input file into chunk, then sent to differenct chunks"""

        in_path = self.gconf.file_path

        file_name = in_path.split('/')[-1]
        self.last_file_id += 1
        server_id = (self.last_data_server_id + 1) % NUM_REPLICATION

        file_length = os.path.getsize(in_path)
        chunks = int(math.ceil(float(file_length) / CHUNK_SIZE))

        # generate chunk, add into <id, chunk> mapping
        self.id_chunk_map[self.last_file_id] = [CHUNK_PATTERN % (self.last_file_id, i) for i in range(chunks)]
        self.id_file_map[self.last_file_id] = (file_name, file_length)

        for i, chunk in enumerate(self.id_chunk_map[self.last_file_id]):
            self.chunk_server_map[chunk] = []

            # copy to 4 data nodes
            for j in range(NUM_REPLICATION):
                assign_server = (server_id + j) % NUM_DATA_SERVER
                self.chunk_server_map[chunk].append(assign_server)

                # add chunk-server info to global variable
                size_in_chunk = CHUNK_SIZE if i < chunks - 1 else file_length % CHUNK_SIZE
                if assign_server not in self.gconf.server_chunk_map:
                    self.gconf.server_chunk_map[assign_server] = []
                self.gconf.server_chunk_map[assign_server].append((chunk, CHUNK_SIZE * i, size_in_chunk))

            server_id = (server_id + NUM_REPLICATION) % NUM_DATA_SERVER

        self.last_data_server_id = (server_id - 1) % NUM_DATA_SERVER
        self.update_meta()

        self.gconf.file_id = self.last_file_id
        for data_event in self.gconf.data_events:
            data_event.set()

    def read(self):
        """assign read mission to each data node"""

        gconf = self.gconf
        file_id = gconf.file_id
        read_offset = gconf.read_offset
        read_count = gconf.read_count

        if file_id not in self.id_file_map:
            print('No such file with id =', file_id)
            gconf.read_event.set()
        elif read_offset < 0 or read_count < 0:
            print('Read offset or count cannot less than 0')
            gconf.read_event.set()
        elif (read_offset + read_count) > self.id_file_map[file_id][1]:
            print('The expected reading exceeds the file, file size:', self.id_file_map[file_id][1])
            gconf.read_event.set()
        else:
            start_chunk = int(math.floor(read_offset / CHUNK_SIZE))
            space_left_in_chunk = (start_chunk + 1) * CHUNK_SIZE - read_offset

            if space_left_in_chunk < read_count:
                print('Cannot read across chunks')
                gconf.read_event.set()
            else:
                # randomly select a data server to read chunk
                read_server_candidates = self.chunk_server_map[CHUNK_PATTERN % (file_id, start_chunk)]
                read_server_id = choice(read_server_candidates)
                gconf.read_chunk = CHUNK_PATTERN % (file_id, start_chunk)
                gconf.read_offset = read_offset - start_chunk * CHUNK_SIZE
                gconf.data_events[read_server_id].set()
                return True

        return False

    def fetch(self):
        """assign download mission"""

        gconf = self.gconf
        file_id = gconf.file_id

        if file_id not in self.id_file_map:
            gconf.fetch_chunks = -1
            print('No such file with id =', file_id)
        else:
            file_chunks = self.id_chunk_map[file_id]
            gconf.fetch_chunks = len(file_chunks)
            # get file's data server
            for chunk in file_chunks:
                gconf.fetch_servers.append(self.chunk_server_map[chunk][0])
            for data_event in gconf.data_events:
                data_event.set()
            return True

        for data_event in gconf.data_events:
            data_event.set()
        # print(gconf.fetch_chunks)
        return None
