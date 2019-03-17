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
NPORT = int(os.environ['NPORT'])
CONFIG_PATH = os.environ['CONF_PATH']

BLOCK_SIZE = 0
REPLICATION = 1
BLOCK_MAP = {}
FILE_TABLE = {}
DATA_NODES = {}


def flush_to_disk():
    pickle.dump(FILE_TABLE, open(CONFIG_PATH + 'ftdata', 'wb'))


def calc_num_blocks(size):
    return int(math.ceil(float(size) / BLOCK_SIZE))


def alloc_blocks(dest, num_blocks):
    blocks = []
    node_ids = []
    for n_data in DATA_NODES.keys():
        if DATA_NODES[n_data][1] == 1:  # if datanode is active only add into sampler
            node_ids.append(n_data)
    rep_num = min(len(node_ids), REPLICATION)
    for i in range(0, num_blocks):
        block_uuid = str(uuid.uuid1())
        active_nodes_ids = random.sample(node_ids, rep_num) #distributes replicas randomly from active nodes array
        blocks.append((block_uuid, active_nodes_ids, i))
        if 'block_info' not in FILE_TABLE[dest]:
            FILE_TABLE[dest]['block_info'] = []
        block_info_list = []
        block_info_list.extend((block_uuid, active_nodes_ids, i))
        FILE_TABLE[dest]['block_info'].append(block_info_list)
    print(FILE_TABLE)
    return blocks


@app.route('/api/v1/getblock', methods=['GET'])
def api_get_block():
    if len(DATA_NODES) == 0:
        return Response("Datanode are not avaliable", status=500)
    filename = request.args.get('file')
    fileType = request.args.get('filetype')
    size = request.args.get('size')
    if exists(filename):
        return Response("File already exist", status=409)  # ignoring for now, will delete it later

    if filename not in FILE_TABLE.keys():
        FILE_TABLE[filename] = {}
        FILE_TABLE[filename]['filetype'] = fileType
    num_blocks = calc_num_blocks(size)
    blocks = alloc_blocks(filename, num_blocks)
    json_string = json.dumps(blocks)
    return Response(json_string, status=200)


@app.route('/api/v1/getblocksize', methods=['GET'])
def api_get_block_size():
    return Response(str(BLOCK_SIZE), status=200)


@app.route('/api/v1/getdatanodes', methods=['GET'])
def api_get_data_node():
    json_string = json.dumps(DATA_NODES)
    return Response(json_string, status=200)


@app.route('/api/v1/readfile', methods=['GET'])
def api_get_read():
    filename = request.args.get('file')
    json_string = json.dumps(FILE_TABLE[filename])
    return Response(json_string, status=200)


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


def blockreport(resp):
    samp = {}
    samp[resp["datanode"]] = resp["blockIds"]
    BLOCK_MAP.update(samp)
    #print("BLOCK_MAP = ", BLOCK_MAP)


@app.route('/blockreport', methods=['POST', ])
def block_report():
    data = request.json
    blockreport(data)
    return Response(None, status=200)


def heartbeat(resp):
    samp = {}
    samp[resp["datanode"]] = [resp["time"], 1]  # 1 consider as active
    DATA_NODES.update(samp)


def update_DataNodes():
    #print("DATA_NODE=", DATA_NODES)
    for n_data in DATA_NODES.keys():
        curTimestamp = int(datetime.utcnow().timestamp())
        lastTimeUpdated = curTimestamp - DATA_NODES[n_data][0]
        if lastTimeUpdated > 15:  #thgis will change according to time
            DATA_NODES[n_data][1] = 0  # 0 will be consider as inactive


@app.route('/heartbeat', methods=['POST', ])
def heart_beat():
    data = request.json
    heartbeat(data)
    return Response(None, status=200)


def update_replica(sourcenode, destinationnode, blockId):
    with app.test_request_context():
        print("REPLICA=", sourcenode, destinationnode, blockId)
        multipart_form_data = {
            'destinationNode': destinationnode,
            'blockId': blockId
        }
        response = requests.post(url='http://' + sourcenode +'/replica', json=multipart_form_data)
        if response.status_code != 200:
            raise Exception(response.text)
        return response.status_code


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
        for block in FILE_TABLE[file]['block_info']:
            nodeids = []
            blockid = block[0]
            for nodeinfo in BLOCK_MAP.keys():
                if (blockid in BLOCK_MAP[nodeinfo]) and (DATA_NODES[nodeinfo][1] == 1):
                    nodeids.append(nodeinfo)
            if len(nodeids) < REPLICATION:
                replicated_nodes = getReplicatedNodeInfo(nodeids, blockid)
                for node in replicated_nodes:
                    res = update_replica(nodeids[0], node, blockid)
                    if res == 200:
                        nodeids.append(node)
            block[1] = nodeids

def exists(file):
    return file in FILE_TABLE

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
