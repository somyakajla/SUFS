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





