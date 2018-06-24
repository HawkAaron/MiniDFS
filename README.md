# Python Implementation of Mini DFS

> Mini-DFS is running through a process. In this process, the name server and data servers are different threads.

## Basic functions of Mini-DFS
*  Read/write a file
	* Upload a file: upload success and return the ID of the file
	* Read the location of a file based on the file ID and the offset

* File striping
	* Slicing a file into several chunks
	* Each chunk is 2MB
	* Uniform distribution of these chunks among four data servers

* Replication
	* Each chunk has three replications
	* Replicas are distributed in different data servers

* Name Server
	* List the relationships between file and chunks
	* List the relationships between replicas and data servers
	* Data server management

* Data Server
	* Read/Write a local chunk
	* Write a chunk via a local directory path

* Client
	* Provide read/write interfaces of a file

## Instructions
1. Run main.py to start:
	`python main.py`
	
2. Commands：
	```bash
	# list all files in DFS, return id, name, size
	MiniDFS > ls
	
	# upload local file to DFS, return file id
	MiniDFS > put source_file_path
	
	# read file in DFS
	MiniDFS > read file_id offset count
	
	# download file in DFS
	MiniDFS > fetch file_id save_path
	
	# exit
	MiniDFS > quit
	```

## Storage
1. `dfs` will be the data storage directory of DFS. `datanode{0,1,2,3}` is the storage dir for each data server, and `namenode` is the dir for name server.

2. Files in DFS are split into several chunks, each named like `0-part-0`.
