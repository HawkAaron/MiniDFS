# -*- coding: utf-8 -*-
import os
import pickle
import math
import threading
import sys
from random import choice
from enum import Enum

# DFS Meta Data
BLOCK_SIZE = 2 * 1024 * 1024
NUM_DATA_SERVER = 4
NUM_REPLICATION = 4
BLOCK_PATTERN = '%s-part-%s'

# Name Node Meta Data
NAME_NODE_META_PATH = './dfs/namenode/meta.pkl'

# Data Node
DATA_NODE_DIR = './dfs/datanode%s'

LS_PATTERN = '%s\t%20s\t%10s'

# Operations
operation_names = ('put', 'read', 'fetch', 'quit', 'ls')
OPERATION = Enum('OPERATION', operation_names)

class GlobalConfig:
    # global variables, shared between Name node and Data nodes
    server_block_map = {} # datanode -> blocks
    read_block = None
    read_offset = None
    read_count = None

    cmd_flag = False
    file_id = None
    file_path = None
    cmd_type = None

    fetch_savepath = None
    fetch_servers = []
    fetch_blocks = None

    # events
    name_event = threading.Event()
    ls_event = threading.Event()
    read_event = threading.Event()

    data_events = []
    main_events = []
    for j in range(NUM_DATA_SERVER):
        data_events.append(threading.Event())
        main_events.append(threading.Event())
