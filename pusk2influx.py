import os
from influxdb_client import InfluxDBClient
import re
from datetime import datetime


def is_float(element: any) -> bool:
    try:
        float(element)
        return True
    except ValueError:
        return False


def time_file(dt: str) -> str:
    r = datetime.strptime(dt, '%d/%m/%Y %H:%M:%S')
    s = (r - datetime(1970, 1, 1)).total_seconds()
    rounded = ((s + 900 / 2) // 900 * 900) - (16 * 60 * 60)
    dt_utc = datetime.fromtimestamp(rounded)
    dt_formatted = dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    return dt_formatted


def pusk_parser(file, ne):
    t = re.compile(r'\*?\[(\d\d/\d\d/\d{4} \d\d:\d\d:\d\d)]=>\[(\w+);\d+]->([^@]+)')
    r = []
    with open(file, 'r') as f:
        for line in f:
            line = line.rstrip('\n')
            if re.match(t, line):
                ls = re.search(t, line)
                dt = ls[1]
                module = ls[2]
                params = ls[3].rstrip(';').split(';')
                par = {}
                for v in params:
                    kv = v.split('=')
                    if is_float(kv[1]):
                        par[kv[0]] = float(kv[1])
                p = {
                    'measurement': measurement,
                    'tags': {
                        'ne': ne,
                        'module': module,
                    },
                    'fields': par,
                    'time': time_file(dt)
                }
                r.append(p)
    return r


if __name__ == '__main__':
    org = 'Telecom'
    bucket = 'Optic'
    token = 'r1aSdBOyZYIguF88n0zOQrNNF59f1fcCeR34bOXvpQR3_NjPoAo3qtifh7ju-jPo0geGjNUrddA4R8LJG19N6A=='
    url = 'http://localhost:8086'
    measurement = 'pusk'

    data_path = os.path.join('data', 'PUSK')
    folders = sorted(os.listdir(data_path))

    with InfluxDBClient(url=url, token=token, org=org) as client:
        with client.write_api() as write_client:
            for folder in folders:
                print(os.path.join(data_path, folder))
                files = sorted(os.listdir(os.path.join(data_path, folder)))
                for file_name in files:
                    data_pusk = pusk_parser(os.path.join(data_path, folder, file_name), folder)
                    write_client.write(bucket, org, data_pusk)
    print('Done.')
