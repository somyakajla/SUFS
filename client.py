import os
import sys
import requests


def getfile(filename, destination):
    res = requests.get(url='http://127.0.0.1:9000/api/v1/readfile?file=' + filename + '')
    if not res:
        print("404: file not found")
        return
    fileinfo = res.json()
    print(fileinfo)
    datanodes = getDataNodes()
    with open(destination, 'w') as fd:
        for f in fileinfo:
            nodes = [datanodes[_] for _ in f[1]]
            for n in nodes:
                url = 'http://' + n[0] + ':' + n[1] + '/readfile?block=' + f[0]
                res = requests.get(url=url)
                #print(data.text)
                if res:
                    fd.write(res.text)
                    break
                else:
                    print("No blocks found. Possibly a corrupt file")


def putfile(source, filename):
    size = os.path.getsize(source)
    blocks = getBlocks(filename, str(size))
    blocksize = getBlockSize()
    print("check block size "+str(blocksize))
    datanodes = getDataNodes()
    with open(source) as f:
        for b in blocks:
            data = f.read(blocksize)
            block_id = b[0]
            nodes = [datanodes[_] for _ in b[1]]
            multipart_form_data = {
                'fileData': data,
                'blockId': block_id
            }
            for n in nodes:
                url = 'http://' + n[0] + ':' + n[1] + '/upload'
                print(url)
                response = requests.post(url, json=multipart_form_data)
                print(response.status_code)


def getBlocks(filename, size):
    r = requests.get(url='http://127.0.0.1:9000/api/v1/getblock?file=' + filename + '&size=' + str(size))
    blocks = r.json()
    return blocks


def getBlockSize():
    r = requests.get(url='http://127.0.0.1:9000/api/v1/getblocksize')
    block_size = int(r.json())
    return block_size


def getDataNodes():
    r = requests.get(url='http://127.0.0.1:9000/api/v1/getdatanodes')
    datanodes = r.json()
    return datanodes

def main(args):
    if args[0] == "getfile":
        getfile(args[1], args[2])
    elif args[0] == "putfile":
        putfile(args[1], args[2])
    else:
        print("try 'put srcFile destFile OR get file'")


if __name__ == "__main__":
    main(sys.argv[1:])
