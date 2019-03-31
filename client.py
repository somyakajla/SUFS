import json
import os
import sys
import requests


NIP = os.environ['NIP']
NPORT = os.environ['NPORT']


'''
    API call to read a file with file name
    filename - name of file wish to read from data nodes
    destination - 
    API call to name node to get the block information and active nodes ids
    where file is stored 
    API call to active data nodes with block Id 
    to get the respective block from that data node
'''
def getfile(args):
    filename = args[1]
    destination = args[2]
    res = requests.get(url='http://' + NIP + ':' + str(NPORT) + '/api/v1/readfile?file=' + filename + '')
    if res.status_code == 404:
        print("404: file not found")
        return
    fileinfo = res.json()
    with open(destination, 'wb') as fd:
        nodes = fileinfo[1]
        for n in nodes:
            url = 'http://' + n + '/api/v1/readfile?block=' + fileinfo[0]
            res = requests.get(url=url, stream=True)
            if res:
                fd.write(res.raw.read())
                break
            else:
                print("No blocks found. Possibly a corrupt file")


# API call to save a file on active data nodes
def putfile(args):
    try:
        source = args[1]
        filename = args[2]
        size = os.path.getsize(source)
        blocks = getBlocks(filename, str(size))
        blocksize = getBlockSize()
        with open(source, 'rb') as f:
            for b in blocks:
                data = f.read(blocksize)
                block_id = b[0]
                payload = {'blockId': block_id}
                multipart_form_data = {
                    'fileData': ('None', data),
                    'filter': (None, json.dumps(payload)) # not able to serialise the bytes into json object hence need to dump into json string
                }
                for n in b[1]:
                    url = 'http://' + n + '/api/v1/upload'
                    response = requests.post(url, files=multipart_form_data)
                    print(response)
    except Exception as error:
        print(error)


''' 
    filename- file name 
    size of that file 
    API call to get the block information from name node
'''
def getBlocks(filename, size):
    r = requests.get(url='http://'+ NIP + ':' + str(NPORT) + '/api/v1/getblock?file=' + filename + '&size=' + str(size))
    if r.status_code != 200:
        raise Exception(r.text)
    blocks = r.json()
    return blocks


# block size from name node
def getBlockSize():
    r = requests.get(url='http://'+ NIP + ':' + str(NPORT) + '/api/v1/getblocksize')
    block_size = int(r.json())
    return block_size


''' 
    main method
    to put file run the command 
    python3 client.py putfile '/Users/somyakajla/Documents/cloudcomputing/h.py' h.py
    to get the file 
    python3 client.py getfile h.py '/Users/somyakajla/Documents/cloudcomputing/hOutput' 
'''
def main(args):
    if args[0] == "getfile":
        getfile(args)
    elif args[0] == "putfile":
        putfile(args)
    else:
        print("try 'put srcFile destFile OR get file'")


if __name__ == "__main__":
    main(sys.argv[1:])
