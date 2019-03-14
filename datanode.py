import configparser
import os
import time
import requests
from flask import request, Flask, Response
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from datetime import datetime

app = Flask(__name__)

DIP = os.environ['DIP'] #'127.0.0.1'#
DPORT = int(os.environ['DPORT'] )   #5000        # an arbitrary UDP port
ROOT_PATH = os.environ['ROOT_PATH'] 
NIP = os.environ['NIP']
NPORT = int(os.environ['NPORT'])



@app.route('/readfile', methods=['GET'])
def read_file():
    r = request.args.get('block')
    block_addr = ROOT_PATH+'/' + str(r)
    print(block_addr)
    if not os.path.isfile(block_addr):
        return Response(status=404)
    f = open(block_addr, 'r')
    return Response(f.read(), status=200)

@app.route('/replica', methods=['POST', ])
def replica_data():
    try:
        data = request.json
        blockId = data['blockId']
        print(blockId)
        nodeId = data['destinationNode']
        print(nodeId)
        block_addr = ROOT_PATH + blockId
        if not os.path.isfile(ROOT_PATH + blockId):
            print("ex")
            return Response(status=404)

        f = open(block_addr, 'r')
        data = f.read()
        multipart_form_data = {
            'fileData': data,
            'blockId': blockId
        }
        url = 'http://' + nodeId + '/upload'
        print(url)
        response = requests.post(url, json=multipart_form_data)
    except Exception as error:
        print(error)
        return Response(error, response.status_code)
    return Response(None, response.status_code)


@app.route('/upload', methods=['POST', ])
def upload_data():
    data = request.json
    with open(ROOT_PATH + str(data['blockId']), 'w') as f:
        f.write(data['fileData'])
    return Response(None, status=200)


def block_report():
    list = [f for f in os.listdir(ROOT_PATH) if not f.startswith('.')]
    multipart_form_data = {
        'datanode': DIP + ':' + str(DPORT),
        'blockIds': list
    }
    try:
        response = requests.post('http://'+ NIP + ':' + str(NPORT) + '/blockreport', json=multipart_form_data)
    except:
        print("remote Ip is not reachable " +  NIP +":" +str(NPORT))


def heartbeat():
    multipart_form_data = {
        'datanode': DIP + ':' + str(DPORT),
        'time': int(datetime.utcnow().timestamp())
    }
    try:
        response = requests.post('http://'+ NIP + ':' + str(NPORT) + '/heartbeat', json=multipart_form_data)
    except:
        print("remote Ip is not reachable " +  NIP +":" +str(NPORT))


def set_conf():
    conf = configparser.ConfigParser()
    conf.readfp(open('py_dfs.conf'))


if __name__ == "__main__":
    set_conf()
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(func=heartbeat, trigger="interval", seconds=5)
    scheduler.add_job(func=block_report, trigger="interval", seconds=5)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    app.run(host=DIP, port=DPORT, debug=True, use_reloader=False)

