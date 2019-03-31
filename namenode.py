import atexit
import configparser
import flask
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import request, Response, json
import math
import uuid
import random
from datetime import datetime
import pickle
import os

app = flask.Flask(__name__)
app.config["DEBUG"] = True

NIP = os.environ['NIP']
NPORT = os.environ['NPORT']
CONFIG_PATH = os.environ['CONFIG_PATH']

BLOCK_SIZE = 0
REPLICATION = 1
BLOCK_MAP = {}
FILE_TABLE = {}
DATA_NODES = {}

''' 
    service which runs in background
    in the interval of 10 sec
    to flush the metadata onto the disk
'''
def flush_to_disk():
    pickle.dump(FILE_TABLE, open(CONFIG_PATH + 'ftdata', 'wb'))


# calculate number of blocks
def calc_num_blocks(size):
    return int(math.ceil(float(size) / BLOCK_SIZE))


''' 
    dest - fileName
    num_blocks - number of blocks a file can be divided into
    node_ids to store active datanode ids 
    replication factor is minimum of (Replication and active datanode numbers)
    block_uuid is globally unique id for each block
    random.sample picks unique active datanode id for each replica
    append block_id , active_node_ids and the sequence number of that block in block array
    return blocks
'''
def alloc_blocks(dest, num_blocks):
    blocks = []
    node_ids = []
    for n_data in DATA_NODES.keys():
        if DATA_NODES[n_data][1] == 1:  # if datanode is active only add into sampler
            node_ids.append(n_data)
    rep_num = min(len(node_ids), REPLICATION)
    for i in range(0, num_blocks):
        block_uuid = str(uuid.uuid1())
        active_nodes_ids = random.sample(node_ids, rep_num)
        blocks.append((block_uuid, active_nodes_ids, i))
        block_info_list = []
        block_info_list.extend((block_uuid, active_nodes_ids, i))
        FILE_TABLE[dest] = block_info_list
    print(FILE_TABLE)
    return blocks


'''
    checks if DATA_NODES data structure is empty then 
    response that no datanodes are available
    checks if file is already in the file table data structure
    response that file is already exists
    file name is not in file table
    calculate number of blocks that file will be split into
    allocate block information into file table data structure
    response block information to client to send it to data node
'''
@app.route('/api/v1/getblock', methods=['GET'])
def api_get_block():
    if len(DATA_NODES) == 0:
        return Response("Datanode are not avaliable", status=500)
    filename = request.args.get('file')
    size = request.args.get('size')
    if exists(filename):
        return Response("File already exist", status=409)  # ignoring for now, will delete it later

    if filename not in FILE_TABLE.keys():
        FILE_TABLE[filename] = {}
    num_blocks = calc_num_blocks(size)
    blocks = alloc_blocks(filename, num_blocks)
    json_string = json.dumps(blocks)
    return Response(json_string, status=200)


''' 
    block size from config file
    send response to client
'''
@app.route('/api/v1/getblocksize', methods=['GET'])
def api_get_block_size():
    return Response(str(BLOCK_SIZE), status=200)


''' 
    filename as request args in url
    response file table dictionary by key file name
'''
@app.route('/api/v1/readfile', methods=['GET'])
def api_get_read():
    filename = request.args.get('file')
    if filename in FILE_TABLE.keys():
        json_string = json.dumps(FILE_TABLE[filename])
        return Response(json_string, status=200)
    return Response(None, status=404)


''' 
    set global variable by reading it from config file 
    make directory on local system to save metadata
    load data on to file table from metadata file
'''
def set_conf():
    conf = configparser.ConfigParser()
    conf.readfp(open('py_dfs.conf'))
    global BLOCK_SIZE, REPLICATION, CONFIG_PATH, DATA_NODES, FILE_TABLE

    BLOCK_SIZE = int(conf.get('NameNode', 'block_size'))
    REPLICATION = int(conf.get('NameNode', 'replication_factor'))
    if not os.path.isdir(CONFIG_PATH):
        os.mkdir(CONFIG_PATH)
    config_data = CONFIG_PATH + 'ftdata'
    if os.path.isfile(config_data):
         FILE_TABLE = pickle.load(open(config_data, 'rb'))


''' 
    update block_map data structure 
    information received from data nodes
'''
def blockreport(resp):
    samp = {}
    samp[resp["datanode"]] = resp["blockIds"]
    BLOCK_MAP.update(samp)


# rcv block report send by active data nodes
@app.route('/api/v1/blockreport', methods=['POST'])
def block_report():
    data = request.json
    blockreport(data)
    return Response(None, status=200)


''' 
    update datanode data structure from heartbeat 
    save time and its status as active(1)
'''
def heartbeat(resp):
    samp = {}
    samp[resp["datanode"]] = [resp["time"], 1]  # 1 consider as active
    DATA_NODES.update(samp)


''' 
    this service will run on interval of 5 seconds
    if the time of last heartbeat rcv of any datanode more than 15 seconds before
    then data node will be considered as inactive 
    and update DATA_NODE data structure
'''
def update_DataNodes():
    for n_data in DATA_NODES.keys():
        curTimestamp = int(datetime.utcnow().timestamp())
        lastTimeUpdated = curTimestamp - DATA_NODES[n_data][0]
        if lastTimeUpdated > 15:  #this will change according to time
            DATA_NODES[n_data][1] = 0  # 0 will be consider as inactive


# rcv heartbeat from data nodes
@app.route('/api/v1/heartbeat', methods=['POST'])
def heart_beat():
    data = request.json
    heartbeat(data)
    return Response(None, status=200)


''' 
    sourcenode - data node id on which block data is present
    destinationnode - data node id on which new replica of that block has to make
    blockId - block id 
    post request to sourcenode with multipart data
    multipart_form_data -  blockId, destinationNode to which block data has to send to make replica
'''
def update_replica(sourcenode, destinationnode, blockId):
    with app.test_request_context():
        print("REPLICA=", sourcenode, destinationnode, blockId)
        multipart_form_data = {
            'destinationNode': destinationnode,
            'blockId': blockId
        }
        response = requests.post(url='http://' + sourcenode +'/api/v1/replica', json=multipart_form_data)
        if response.status_code != 200:
            raise Exception(response.text)
        return response.status_code


''' 
    odeids - id of data node which have respective block with blockId
    calculate how many replica needs to be make for block
    replica should not be save on the same data nodes which already have that block
    data node should active 
    return data nodes id on which replica should be save in case of less number replicas of block
'''
def getReplicatedNodeInfo(nodeids, blockid):
    available_replica = len(nodeids)
    replication_count = REPLICATION - available_replica
    result_nodes = []
    for node in DATA_NODES.keys():
        if (node not in nodeids) and (DATA_NODES[node][1] == 1) and (blockid not in BLOCK_MAP[node]):
            result_nodes.append(node)
            if replication_count == 0:
                return result_nodes
            replication_count -= 1
    return result_nodes


''' 
    runs on an interval of every 25 seconds
    Removes the node id and its corresponding array of blocks from  BLOCK_MAP which are inactive in DATA_NODES dictionary
    For each block of each file in FILE_TABLE, 
    it checks that number of active node holding that block is less than the number of replica.
    If so, it send request to the datanode which has the respective block 
    it sends the information of active datanode ids which are active 
    and currently not having the replica of that block 
    and block Id as well. 
    then data node send request to each active data node to store the block.
    update meta data table with new replica node ids
'''
def syncFileTable():
    print("BLOCK MAP =", BLOCK_MAP)
    print("DATA_NODE =", DATA_NODES)
    print("FILE_TABLE =", FILE_TABLE)
    node_del = []
    for node in BLOCK_MAP.keys():
        if DATA_NODES[node][1] != 1:
            node_del.append(node)
    for n in node_del:
        del BLOCK_MAP[n]

    for file in FILE_TABLE.keys():
        nodeids = []
        blockid = FILE_TABLE[file][0]
        for nodeinfo in BLOCK_MAP.keys():
            if (blockid in BLOCK_MAP[nodeinfo]) and (DATA_NODES[nodeinfo][1] == 1):
                nodeids.append(nodeinfo)
        if len(nodeids) < REPLICATION and len(nodeids) < len(DATA_NODES.keys()):
            replicated_nodes = getReplicatedNodeInfo(nodeids, blockid)
            for node in replicated_nodes:
                res = update_replica(nodeids[0], node, blockid)
                if res == 200:
                    nodeids.append(node)
        FILE_TABLE[file][1] = nodeids


#   checks file exixts in filetable
def exists(file):
    return file in FILE_TABLE


''' 
    main function
    three jobs are running in background 
    atexit to close/shutdown the background tasks
'''
if __name__ == "__main__":
    set_conf()
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(func=update_DataNodes, trigger="interval", seconds=5)
    scheduler.add_job(func=flush_to_disk, trigger="interval", seconds=10)
    scheduler.add_job(func=syncFileTable, trigger="interval", seconds=25)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    atexit.register(flush_to_disk)
    app.run(host=NIP, port=NPORT, debug=True, use_reloader=False)
