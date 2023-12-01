import os
from influxdb_client import InfluxDBClient
from datetime import datetime


def v(string, start, n_bytes=2):
    # Value from byte string
    return int.from_bytes(string[start:start + n_bytes], byteorder='little')


def opt(file):
    result = [['time', 'ne', 'object', 'param', 'last', 'min', 'max']]
    with open(file, 'rb') as f:
        s = f.read()
    n = s.find(b'\x0a')
    while n < len(s):
        if v(s, n + 1) != 2:
            n += 46
            continue  # Skip data with "Disconnected NE 0x0A...00000000"
        time = (v(s, n + 23, 4) + 450) // 900 * 900
        ne = v(s, n + 5)
        object_id = v(s, n + 9)
        count = v(s, n + 27)
        for i in range(int(count / 3)):
            param_id = v(s, n + 29 + i * 26)
            r = [time, ne, object_id, param_id]
            for j in range(3):
                abs_value = v(s, n + 31 + i * 26 + j * 8)
                negative = v(s, n + 33 + i * 26 + j * 8)
                _ = v(s, n + 35 + i * 26 + j * 8)  # I don't know, 6 or 7
                degree = v(s, n + 37 + i * 26 + j * 8)
                value = abs_value / degree
                if negative == 1:
                    value *= -1
                r.append(value)
            result.append(r)
        n = n + 14 * 2 + int(count / 3) * 13 * 2
    return result


def time_record(count: int):
    time = datetime.fromtimestamp(count - 8 * 60 * 60)
    time = time.strftime('%Y-%m-%dT%H:%M:%SZ')
    return time


def xdm_parser(file):
    r = []
    data = opt(file)
    for row in data[1:]:
        p = {
            'measurement': measurement,
            'tags': {
                'ne': row[1],
                'object': row[2],
                'param': row[3]
            },
            'fields': {
                'last': row[4],
                'min': row[5],
                'max': row[6]
            },
            'time': time_record(int(row[0]))
        }
        r.append(p)
    return r


if __name__ == '__main__':
    org = 'Telecom'
    bucket = 'Optic'
    token = 'r1aSdBOyZYIguF88n0zOQrNNF59f1fcCeR34bOXvpQR3_NjPoAo3qtifh7ju-jPo0geGjNUrddA4R8LJG19N6A=='
    url = 'http://localhost:8086'
    measurement = 'xdm'

    data_path = 'data/XDM/'
    files = sorted(os.listdir(data_path))

    with InfluxDBClient(url=url, token=token, org=org) as client:
        with client.write_api() as write_client:
            for file_name in files:
                print(file_name)
                data_xdm = xdm_parser(data_path + file_name)
                write_client.write(bucket, org, data_xdm)
    print('Done.')
