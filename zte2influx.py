import os
import zipfile
import csv
from io import TextIOWrapper
from influxdb_client import InfluxDBClient
from datetime import datetime
import re


def str_time(t):
    # 2023-06-17 00:00:00.000
    dt = datetime.strptime(t, "%Y-%m-%d %H:%M:%S.%f").timestamp() - 8 * 60 * 60
    return datetime.fromtimestamp(dt).strftime('%Y-%m-%dT%H:%M:%SZ')


def read_zip_file(z, r, name, offset=0):
    with z.open(name, 'r') as f:
        file_reader = csv.reader(TextIOWrapper(f, 'utf-8'), delimiter='\t')
        for row in file_reader:
            if row[7 + offset][-7:] == '=401_1}' or row[7 + offset][-7:] == '=402_1}':
                template = re.compile(r'BN:ME\{(\d+)},EQ=\{/r=(\d+)/sh=(\d+)/sl=(\d+)},PTP={/p=40[12]_1}')
                tags = re.search(template, row[7 + offset])
                if row[7 + offset][-7:] == '=401_1}':
                    fields = {'Tx': float(row[11 + offset])}
                else:
                    fields = {'Rx': float(row[15 + offset])}
                p = {
                    'measurement': measurement,
                    'tags': {
                        'NE': int(tags[1]),
                        'EQ': f'[{tags[2]}-{tags[3]}-{tags[4]}]',
                    },
                    'fields': fields,
                    'time': str_time(row[3])
                }
                r.append(p)


def read_zip(file):
    with zipfile.ZipFile(data_path + file) as z:
        r = []
        read_zip_file(z, r, 'BNPM.dbo.PMP_04701.dat', offset=1)
        read_zip_file(z, r, 'BNPM.dbo.PMP_04701_D.dat')
        return r


if __name__ == '__main__':
    org = 'Telecom'
    bucket = 'Optic'
    token = 'r1aSdBOyZYIguF88n0zOQrNNF59f1fcCeR34bOXvpQR3_NjPoAo3qtifh7ju-jPo0geGjNUrddA4R8LJG19N6A=='
    url = 'http://localhost:8086'
    measurement = 'zte'

    data_path = 'data/ZTE/'
    files = sorted(os.listdir(data_path))

    with InfluxDBClient(url=url, token=token, org=org) as client:
        with client.write_api() as write_client:
            for file_name in files:
                print(file_name)
                write_client.write(bucket, org, read_zip(file_name))
    print('Done.')
