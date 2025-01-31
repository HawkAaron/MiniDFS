# -*- coding: utf-8 -*-
import os
import pickle
import math
import threading
import sys
from random import choice
from enum import Enum

# DFS Meta Data
CHUNK_SIZE = 2 * 1024 * 1024
NUM_DATA_SERVER = 4
NUM_REPLICATION = 4
CHUNK_PATTERN = '%s-part-%s'

# Name Node Meta Data
NAME_NODE_META_PATH = './dfs/namenode/meta.pkl'

# Data Node
DATA_NODE_DIR = './dfs/datanode%s'

LS_PATTERN = '%s\t%20s\t%10s'

# Operations
operation_names = ('put', 'read', 'fetch', 'quit', 'ls', 'put2', 'read2', 'fetch2', 'ls2', 'mkdir')
COMMAND = Enum('COMMAND', operation_names)

class GlobalConfig:
    # global variables, shared between Name node and Data nodes
    server_chunk_map = {} # datanode -> chunks
    read_chunk = None
    read_offset = None
    read_count = None

    cmd_flag = False
    file_id = None
    file_dir = None # read or fetch using dir/filename
    file_path = None # local source path
    cmd_type = None

    put_savepath = None # put2 save using dir on DFS
    fetch_savepath = None # download on local
    fetch_servers = []
    fetch_chunks = None

    # events
    name_event = threading.Event()
    ls_event = threading.Event()
    read_event = threading.Event()
    mkdir_event = threading.Event()

    data_events = [threading.Event() for i in range(NUM_DATA_SERVER)]
    main_events = [threading.Event() for i in range(NUM_DATA_SERVER)]
