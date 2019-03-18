# SUFS
Seattle University File System

# To Run Nameserve 
python namesever.py

These environment variable needs to be setup in each datanode with ip address and port and ROOT_Path to save block_files.

# DataNode Environment Variable Setup
export DIP=127.0.0.1
export DPORT=5000
export ROOT_PATH=/Users/somyakajla/Documents/store/DATANODE2

# to data node server
python datanode.py

To Run Client:
#To Upload file
python client.py putfile source_path dest_filename

#To retrieve file
python client.py getfile source_filename destination_path

Functionality Description : 

NameNode - 

In this class, We have created 3 data structure namely, FILE_TABLE, DATA_NODES, BLOCK_MAP.

FILE_TABLE - { filename: { filetype : text/image, blockinfo : [ [ blockid, [ array of datanodes ], sequence ],[ blockid2, [ array of datanodes ], sequence ] ] } }

DATA_NODES - { nodeId : [ timestamp, 0/1(inactive/active) ] }

BLOCK_MAP - { nodeid : [ array of blockids ] }
A. There are 3 services running in the background to perform following tasks.
   1. update_DataNodes : it checks in  DATA_NODES dictionary that the difference in timestamp and current time has a value   more than 15 seconds then update that node as a inactive node.
   2. Flush_to_disk : every 10 seconds it updates the FILE_TABLE dictionary in disk’s directory.
   3. syncFileTable : every 25 second, this service runs in the background which does following steps- 
       a. Removes the nodeid and its corresponding array of blocks from  BLOCK_MAP which is inactive in DATA_NODES dictionary.
       b. For each block of each file in FILE_TABLE, it checks that number of active node holding that block is less than the   number of replica. 
       c. If so, it send request to the datanode which has the respective block containing the information of active datanode ids which are active and currently not having the replica of that block as well as block Id. 
       d. then data node send request to each active data node to store the block.
       e. Update the FILE_TABLE with new data nodes corresponding to block.
B. FILE_TABLE dictionary is being created at the time when client send request to namenode to get the information about datanodes and blocks where client can store the respective file. 
C. DATA_NODES dictionary is being created by the heartbeat send by data nodes.
D. BLOCK_MAP dictionary is being created/updated by block report send by data nodes.

Important- Namenode does have any information about its datanodes untill datanodes itself sends its heartbeat and block reports to the name node.







