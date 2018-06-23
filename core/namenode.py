from .common import *

# def add_block_2_server(server_block_map, server_id, block, offset, count):

#     if server_id not in server_block_map:
#         server_block_map[server_id] = []
#     server_block_map[server_id].append((block, offset, count))


class NameNode(threading.Thread):
    """
    Name Server，handle instructions and manage data servers
    Client can use `ls, read, fetch` cmds.
    """
    def __init__(self, name, gconf):
        super(NameNode, self).__init__(name=name)
        self.gconf = gconf # global parameters
        self.metas = None
        self.id_block_map = None # file id -> block, eg. {0: ['0-part-0'], 1: ['1-part-0']}
        self.id_file_map = None # file id -> name, eg. {0: ('README.md', 1395), 1: ('mini_dfs.py', 14603)}
        self.block_server_map = None # block -> data servers, eg. {'0-part-0': [0, 1, 2], '1-part-0': [0, 1, 2]}
        self.last_file_id = -1 # eg. 1
        self.last_data_server_id = -1 # eg. 2
        self.load_meta()

    def run(self):
        gconf = self.gconf
        while True:
            # waiting for cmds
            gconf.name_event.wait()

            if gconf.cmd_flag:
                if gconf.cmd_type == OPERATION.put:
                    self.generate_split()
                elif gconf.cmd_type == OPERATION.read:
                    self.assign_read_work()
                elif gconf.cmd_type == OPERATION.fetch:
                    self.assign_fetch_work()
                elif gconf.cmd_type == OPERATION.ls:
                    self.list_dfs_files()
                else:
                    pass
            gconf.name_event.clear()

    def load_meta(self):
        """load Name Node Meta Data"""

        if not os.path.isfile(NAME_NODE_META_PATH):
            self.metas = {
                'id_block_map': {},
                'id_len_map': {},
                'block_server_map': {},
                'last_file_id': -1,
                'last_data_server_id': -1
            }
        else:
            with open(NAME_NODE_META_PATH, 'rb') as f:
                self.metas = pickle.load(f)
        self.id_block_map = self.metas['id_block_map']
        self.id_file_map = self.metas['id_len_map']
        self.block_server_map = self.metas['block_server_map']
        self.last_file_id = self.metas['last_file_id']
        self.last_data_server_id = self.metas['last_data_server_id']

    def update_meta(self):
        """update Name Node Meta Data after put"""

        with open(NAME_NODE_META_PATH, 'wb') as f:
            self.metas['last_file_id'] = self.last_file_id
            self.metas['last_data_server_id'] = self.last_data_server_id
            pickle.dump(self.metas, f)

    def list_dfs_files(self):
        """ls print meta data info"""

        print('total', len(self.id_file_map))
        for file_id, (file_name, file_len) in self.id_file_map.items():
            print(LS_PATTERN % (file_id, file_name, file_len))
        self.gconf.ls_event.set()

    def generate_split(self):
        """split input file into block, then sent to differenct blocks"""

        in_path = self.gconf.file_path

        file_name = in_path.split('/')[-1]
        self.last_file_id += 1
        server_id = (self.last_data_server_id + 1) % NUM_REPLICATION

        file_length = os.path.getsize(in_path)
        blocks = int(math.ceil(float(file_length) / BLOCK_SIZE))

        # generate block, add into <id, block> mapping
        self.id_block_map[self.last_file_id] = [BLOCK_PATTERN % (self.last_file_id, i) for i in range(blocks)]
        self.id_file_map[self.last_file_id] = (file_name, file_length)

        for i, block in enumerate(self.id_block_map[self.last_file_id]):
            self.block_server_map[block] = []

            # copy to 4 data nodes
            for j in range(NUM_REPLICATION):
                assign_server = (server_id + j) % NUM_DATA_SERVER
                self.block_server_map[block].append(assign_server)

                # add block-server info to global variable
                size_in_block = BLOCK_SIZE if i < blocks - 1 else file_length % BLOCK_SIZE
                if assign_server not in self.gconf.server_block_map:
                    self.gconf.server_block_map[assign_server] = []
                self.gconf.server_block_map[assign_server].append((block, BLOCK_SIZE * i, size_in_block))

            server_id = (server_id + NUM_REPLICATION) % NUM_DATA_SERVER

        self.last_data_server_id = (server_id - 1) % NUM_DATA_SERVER
        self.update_meta()

        self.gconf.file_id = self.last_file_id
        for data_event in self.gconf.data_events:
            data_event.set()

    def assign_read_work(self):
        """assign read mission to each data node"""

        gconf = self.gconf
        file_id = gconf.file_id
        read_offset = gconf.read_offset
        read_count = gconf.read_count

        if file_id not in self.id_file_map:
            print('No such file with id =', file_id)
            gconf.read_event.set()
        elif read_offset < 0 or read_count:
            print('Read offset or count cannot less than 0')
            gconf.read_event.set()
        elif (read_offset + read_count) > self.id_file_map[file_id][1]:
            print('The expected reading exceeds the file, file size:', self.id_file_map[file_id][1])
            gconf.read_event.set()
        else:
            start_block = int(math.floor(read_offset / BLOCK_SIZE))
            space_left_in_block = (start_block + 1) * BLOCK_SIZE - read_offset

            if space_left_in_block < read_count:
                print('Cannot read across blocks')
                gconf.read_event.set()
            else:
                # randomly select a data server to read block
                read_server_candidates = self.block_server_map[BLOCK_PATTERN % (file_id, start_block)]
                read_server_id = choice(read_server_candidates)
                gconf.read_block = BLOCK_PATTERN % (file_id, start_block)
                gconf.read_offset = read_offset - start_block * BLOCK_SIZE
                gconf.data_events[read_server_id].set()
                return True

        return False

    def assign_fetch_work(self):
        """assign download mission"""

        gconf = self.gconf
        file_id = gconf.file_id

        if file_id not in self.id_file_map:
            print('No such file with id =', file_id)
        else:
            file_blocks = self.id_block_map[file_id]
            gconf.fetch_blocks = len(file_blocks)
            # get file's data server
            for block in file_blocks:
                gconf.fetch_servers.append(self.block_server_map[block][0])
            for data_event in gconf.data_events:
                data_event.set()
            return True

        for data_event in gconf.data_events:
            data_event.set()
        return None
