import json
import os
import requests
from flask import request, Flask, Response
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from datetime import datetime

app = Flask(__name__)

DIP = os.environ['DIP']
DPORT = os.environ['DPORT']
ROOT_PATH = os.environ['ROOT_PATH']
NIP = os.environ['NIP']
NPORT = os.environ['NPORT']



@app.route('/api/v1/readfile', methods=['GET'])
def read_file():
    r = request.args.get('block')
    block_addr = ROOT_PATH+'/' + str(r)
    if not os.path.isfile(block_addr):
        return Response(status=404)
    f = open(block_addr, 'rb')
    return Response(f.read(), status=200)

@app.route('/api/v1/replica', methods=['POST', ])
def replica_data():
    try:
        data = request.json
        blockId = data['blockId']
        nodeId = data['destinationNode']
        block_addr = ROOT_PATH + blockId
        if not os.path.isfile(ROOT_PATH + blockId):
            return Response(status=404)

        f = open(block_addr, 'rb')
        data = f.read()
        payload = {'blockId': blockId}
        multipart_form_data = {
            'fileData': ('None', data),
            'filter': (None, json.dumps(payload))
        }
        url = 'http://' + nodeId + '/api/v1/upload'
        response = requests.post(url, files=multipart_form_data)
    except Exception as error:
        print(error)
        return Response(error, response.status_code)
    return Response(None, response.status_code)


@app.route('/api/v1/upload', methods=['POST', ])
def upload_data():
    data = request.files['fileData'].read()
    blockId = json.loads(request.form['filter'])
    with open(ROOT_PATH + str(blockId['blockId']), 'wb') as f:
        f.write(data)
    return Response(None, status=200)


def block_report():
    list = [f for f in os.listdir(ROOT_PATH) if not f.startswith('.')]
    multipart_form_data = {
        'datanode': DIP + ':' + str(DPORT),
        'blockIds': list
    }
    try:
        response = requests.post('http://' + NIP + ':' + str(NPORT) + '/api/v1/blockreport', json=multipart_form_data)
    except:
        print("remote Ip is not reachable " + NIP + ":" + str(NPORT))


def heartbeat():
    multipart_form_data = {
        'datanode': DIP + ':' + str(DPORT),
        'time': int(datetime.utcnow().timestamp())
    }
    try:
        response = requests.post('http://'+ NIP + ':' + str(NPORT) + '/api/v1/heartbeat', json=multipart_form_data)
    except:
        print("remote Ip is not reachable " + NIP + ":" + str(NPORT))


def set_conf():
    if not os.path.isdir(ROOT_PATH):
        os.mkdir(ROOT_PATH)


if __name__ == "__main__":
    set_conf()
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(func=heartbeat, trigger="interval", seconds=5)
    scheduler.add_job(func=block_report, trigger="interval", seconds=5)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    app.run(host=DIP, port=DPORT, debug=True, use_reloader=False)

