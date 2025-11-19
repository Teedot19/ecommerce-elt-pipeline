import csv
from datetime import datetime, date, timedelta

data = [
    {'timestamp': '2024-01-01 10:00:00', 'user': 'alice', 'action': 'login'},
    {'timestamp': '2024-01-01 10:05:10', 'user': 'bob', 'action': 'login'},
    {'timestamp': '2024-01-01 10:07:55', 'user': None, 'action': 'logout'},
    {'timestamp': '2024-01-01 10:09:30', 'user': 'carol', 'action': 'bad_line'},
    {'timestamp': '2024-01-01 10:12:55', 'user': 'dan', 'action': 'logout'}
]


with open('logs.csv','w',newline = '') as out_file:

    writer = csv.DictWriter(out_file, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)


def extract_csv(path):
    import csv
    with open(path,'r') as out_file:
        return list(csv.DictReader(out_file))


def parse_rows(rows):

    parsed = []

    for row in rows:
        r2=row.copy()
        datetime  = datetime.strptime(r2['timestamp'],"%Y-%m-%d %H:%M:%S")
        # r2['datetime'] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        r2['datetime'] = datetime
        parsed.append(r2)
    return parsed


def validate_rows(rows):
    valid = []

    for row in rows:
        if row['user'] == '':
            row['user'] = 'none'
        valid.append(row)
    return valid


def transform(rows):
    output = []

    for row in rows:
        row['hour']= row['datetime'].hour
        output.append(row)
    return output


def load_csv(path,rows):
    with open(path,'w',newline = '') as out_file:
        writer = csv.DictWriter(out_file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)



def run_pipeline(input_path, output_path):
    raw = extract_csv(input_path)
    parsed = parse_rows(raw)
    validated = validate_rows(parsed)
    transformed = transform(validated)
    load = load_csv(output_path,transformed)

    return transformed