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

* Directory management
	* Write a file in a given directory
	* Access a file via "directory + file name"

* Name Server
	* List file tree
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

	# list file tree in DFS
	MiniDFS > ls2
	
	# make dir in DFS
	MiniDFS > mkdir file_dir

	# upload local file to DFS, return file id
	MiniDFS > put source_file_path

	# upload local file to DFS dir, return file id
	MiniDFS > put2 source_file_path dest_dir

	# read file in DFS
	MiniDFS > read file_id offset count
	
	# read file using dir
	MiniDFS > read2 dir/file_name offset count

	# download file in DFS
	MiniDFS > fetch file_id save_path
	
	# download using dir
	MiniDFS > fetch2 dir/file_name save_path

	# exit
	MiniDFS > quit
	```

## Example
```bash
python main.py

MiniDFS > ls

MiniDFS > ls2

# upload file
MiniDFS > put ptb.chr

MiniDFS > put ptb.wrd

MiniDFS > put main.py

# make dir
MiniDFS > mkdir dir1

MiniDFS > mkdir dir2

MiniDFS > mkdir dir3/dir1/dir1

# put using filename
MiniDFS > put2 ptb.wrd dir1

MiniDFS > put2 ptb.wrd dir3/dir1

MiniDFS > put2 main.py dir3/dir1/dir1

# read using file id
MiniDFS > read 0 0 10

# read using dir/name
MiniDFS > read2 ptb.wrd 0 10

MiniDFS > read2 dir1/ptb.wrd 0 10

MiniDFS > read2 dir3/dir1/dir1/main.py 0 100

# download using file id
MiniDFS > fetch 0 0

MiniDFS > fetch2 dir3/dir1/dir1/main.py 1

MiniDFS > fetch2 dir1/ptb.wrd 2

# check file tree
MiniDFS > ls

MiniDFS > ls2

# exit
MiniDFS > quit

# diff file
diff 0 ptb.chr
diff 1 main.py
diff 2 ptb.wrd

# check data nodes md5
./check.sh

```

## Storage
1. `dfs` will be the data storage directory of DFS. `datanode{0,1,2,3}` is the storage dir for each data server, and `namenode` is the dir for name server.

2. Files in DFS are split into several chunks, each named like `0-part-0`.
