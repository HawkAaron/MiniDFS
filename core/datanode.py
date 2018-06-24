from .common import *

class DataNode(threading.Thread):
    """Data Server: execute command from nameserver."""
    
    def __init__(self, server_id, gconf):
        super(DataNode, self).__init__(name='DataServer%s' % (server_id,))
        self.gconf = gconf
        self._server_id = server_id

    def run(self):
        gconf = self.gconf
        while True:
            gconf.data_events[self._server_id].wait()
            if gconf.cmd_flag:
                if gconf.cmd_type == COMMAND.put and self._server_id in gconf.server_chunk_map:
                    self.save()
                elif gconf.cmd_type == COMMAND.read:
                    self.read()
                else:
                    pass
            gconf.data_events[self._server_id].clear()
            gconf.main_events[self._server_id].set()

    def save(self):
        """Data Node save file"""

        data_node_dir = DATA_NODE_DIR % (self._server_id,)
        with open(self.gconf.file_path, 'r') as f_in:
            for chunk, offset, count in self.gconf.server_chunk_map[self._server_id]:
                f_in.seek(offset, 0)
                content = f_in.read(count)

                with open(data_node_dir + os.path.sep + chunk, 'w') as f_out:
                    f_out.write(content)
                    f_out.flush()

    def read(self):
        """read chunk according to offset and count"""

        read_path = (DATA_NODE_DIR % (self._server_id,)) + os.path.sep + self.gconf.read_chunk

        with open(read_path, 'r') as f_in:
            f_in.seek(self.gconf.read_offset)
            content = f_in.read(self.gconf.read_count)
            print(content)
        self.gconf.read_event.set()