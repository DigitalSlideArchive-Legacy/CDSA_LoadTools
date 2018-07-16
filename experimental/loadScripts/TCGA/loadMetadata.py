# Load metadata into Girders
#  Author: Jonathan Beezley
# Before running this script you must have the TCGA metadata flat files downaloded.
#
# Usage: python loadMetadata.py METADATA_ROOT_DIR
# Data is loaded directly into Mongodb and host is assumed to be localhost
# If you are running this in a docker image, you may want to change localhost
# to the mongoDB IP address (if they are on different docker images)
#
# Data is macthed to the patient/case based on the filename
# Data will be stored for each case under tcga/meta key

from __future__ import print_function

import re
import sys
import os
import csv

from pymongo import MongoClient


# http://stackoverflow.com/a/13993400
# guarantee unicode string
def _u(t):
    return t.decode('UTF-8', 'replace') if isinstance(t, str) else t


def _uu(*tt):
    return tuple(_u(t) for t in tt)


# guarantee byte string in UTF8 encoding
def _u8(t):
    return t.encode('UTF-8', 'replace') if isinstance(t, unicode) else t


def _uu8(*tt):
    tuple(_u8(t) for t in tt)


# dict to store patient -> uuid mapping
uuid_map = {}

re_barcode = re.compile(
    '^tcga-[0-9a-z]{2}-[0-9a-z]{4}',
    re.I
)


def load_csv(file_name):
    with open(file_name, 'r') as f:
        reader = csv.reader(f, delimiter='\t')

        header = reader.next()
        data = []
        for row in reader:
            obj = {}
            for i, key in enumerate(header):
                if _u(row[i]).strip().lower() != '[not available]':
                    obj[_u(key)] = _u(row[i])
            data.append(obj)

    return data


def load_all(path):
    all_files = {}
    for root, dirs, files in os.walk(path):
        print('Loading %s...' % root, file=sys.stderr)
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_key = os.path.splitext(file_name)[0].replace('.', '-')
            try:
                all_files[_u(file_key)] = load_csv(file_path)
            except Exception:
                print('Failed to load %s' % file_path, file=sys.stderr)

    return all_files


def get_barcode(record):
    for key in record:
        value = record[key]
        match = re_barcode.match(value)
        if match:
            return match.group()


def merge(all_files):
    global uuid_map
    print('Merging records...', file=sys.stderr)
    merged = {}
    for file in all_files:
        data = all_files[file]

        for d in data:
            patient = get_barcode(d)
            if not patient:
                print('Invalid record in  %s' % file, file=sys.stderr)
                continue

            if 'bcr_patient_uuid' in d:
                id = d['bcr_patient_uuid'].lower()
                if patient in uuid_map and uuid_map[patient] != id:
                    raise Exception('Duplicate barcode, uuid pair')
                uuid_map[patient] = id

            if not patient:
                print('Skipping empty record in %s' % file, file=sys.stderr)
                continue

            r = merged.get(patient, {})
            r[file] = d
            merged[patient] = r

    return merged


def insert(metadata, host='localhost', port=27017, dbname='girder'):
    print('Importing into database %s' % dbname, file=sys.stderr)

    folder = MongoClient(host=host, port=port)[dbname].folder
    for patient in metadata:
        meta = metadata[patient]
        doc = folder.find_one({'name': patient})
        if doc is None:
            print('Skipping barcode %s' % patient, file=sys.stderr)
            continue

        tcga = doc.get('tcga', {})
        tcga['meta'] = meta

        if patient in uuid_map:
            tcga['uuid'] = uuid_map[patient]
        else:
            print('No UUID for %s' % patient)

        doc['tcga'] = tcga
        folder.replace_one({'_id': doc['_id']}, doc)


def main(path):
    merged = merge(load_all(path))
    insert(merged)


if __name__ == '__main__':
    main(sys.argv[1])