from datetime import datetime, timezone
import os
import csv
from influxdb_client import InfluxDBClient


def is_float(element: any) -> bool:
    try:
        float(element)
        return True
    except ValueError:
        return False


def time_file(filename: str) -> str:
    # LaserPerformance20230331075358.csv
    date = filename[-18:-10]  # 20230103
    time = filename[-10:-4]  # 073311
    r = datetime.strptime(date + time, "%Y%m%d%H%M%S")
    s = r.timestamp()
    rounded = (s + 900 / 2) // 900 * 900 - 8 * 60 * 60
    dt_utc = datetime.fromtimestamp(rounded)
    dt_formatted = dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    return dt_formatted


def read_csv(file):
    with open(data_path + file, encoding='utf-8') as file_open:
        r = []
        file_reader = csv.reader(file_open)
        next(file_reader)
        for row in file_reader:
            fields = {}
            if is_float(row[3]):
                fields['Tx'] = float(row[3])
            if is_float(row[4]):
                fields['Rx'] = float(row[4])
            if is_float(row[5]):
                fields['Bias'] = float(row[5])
            if is_float(row[6]):
                fields['Temperature'] = float(row[6])
            if is_float(row[7]):
                fields['Voltage'] = float(row[7])
            if fields:
                p = {
                    'measurement': measurement,
                    'tags': {
                        'NE': row[0],
                        'Card': row[1],
                        'Object': row[2]
                    },
                    'fields': fields,
                    'time': time_file(file)
                }
                r.append(p)
        return r


if __name__ == '__main__':
    org = 'Telecom'
    bucket = 'Optic'
    token = 'r1aSdBOyZYIguF88n0zOQrNNF59f1fcCeR34bOXvpQR3_NjPoAo3qtifh7ju-jPo0geGjNUrddA4R8LJG19N6A=='
    url = 'http://localhost:8086'
    measurement = 'bg'

    data_path = 'data/BG/'
    files = sorted(os.listdir(data_path))

    with InfluxDBClient(url=url, token=token, org=org) as client:
        with client.write_api() as write_client:
            for file_name in files:
                print(file_name)
                write_client.write(bucket, org, read_csv(file_name))
    print('Done.')
