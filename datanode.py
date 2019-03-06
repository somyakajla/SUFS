import os

from flask import request, Flask, Response, json, send_from_directory

app = Flask(__name__)

DIP = os.environ['DIP']
DPORT = int(os.environ['DPORT'] )           # an arbitrary UDP port
ROOT_PATH = os.environ['ROOT_PATH']


@app.route('/readfile', methods=['GET'])
def read_file():
    r = request.args.get('block')
    block_addr = ROOT_PATH+'/' + str(r)
    print(block_addr)
    if not os.path.isfile(block_addr):
        return Response(status=404)
    f = open(block_addr, 'r')
    return Response(f.read(), status=200)


@app.route('/upload', methods=['POST', ])
def upload_data():
    data = request.json
    with open(ROOT_PATH + '/' + str(data['blockId']), 'w') as f:
        f.write(data['fileData'])
    return Response(data['blockId'], status=200)


if __name__ == "__main__":
    app.run(host=DIP, port=DPORT, debug=True)
