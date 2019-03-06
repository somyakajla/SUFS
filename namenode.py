import configparser
import flask
from flask import request, jsonify, Response, json
import math
import uuid
import random

app = flask.Flask(__name__)
app.config["DEBUG"] = True

NIP = "127.0.0.1"
NPORT = 9000
BLOCK_SIZE = 0
REPLICATION = 1
BLOCK_MAP = {}
FILE_TABLE = {}
DATA_NODES = {}
CONFIG_PATH = ""


def calc_num_blocks(size):
    return int(math.ceil(float(size) / BLOCK_SIZE))


def alloc_blocks(dest, num_blocks):
    blocks = []
    for i in range(0, num_blocks):
        block_uuid = uuid.uuid1()
        nodes_ids = random.sample(DATA_NODES.keys(), REPLICATION)
        blocks.append((block_uuid, nodes_ids))
        FILE_TABLE[dest].append((block_uuid, nodes_ids))
    return blocks


@app.route('/api/v1/getblock', methods=['GET'])
def api_get_block():
    filename = request.args.get('file')
    size = request.args.get('size')
    if exists(filename):
        pass  # ignoring for now, will delete it later
    FILE_TABLE[filename] = []
    num_blocks = calc_num_blocks(size)
    print(num_blocks)
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
    global BLOCK_SIZE, REPLICATION, CONFIG_PATH, DATA_NODES

    BLOCK_SIZE = int(conf.get('NameNode', 'block_size'))
    REPLICATION = int(conf.get('NameNode', 'replication_factor'))
    CONFIG_PATH = str(conf.get('NameNode', 'config_path'))
    datanodes = conf.get('NameNode', 'datanodes').split(',')
    for m in datanodes:
        id, host, port = m.split(":")
        DATA_NODES[id] = (host, port)
    #print(DATA_NODES)


def exists(file):
    return file in FILE_TABLE


if __name__ == "__main__":
    set_conf()
    app.run(host=NIP, port=NPORT)
