#/usr/bin/env python

import gevent.monkey
gevent.monkey.patch_all()

import boto

import random
import string
import time
import sys

import gevent

DYNAMODB_TABLE = 'CodeChallenge'


def random_string(N=10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits)
            for x in range(N))


def create_random_item(table):
    item_data = {
            'content': random_string()
    }
    item = table.new_item(
        hash_key=random_string(),
        attrs=item_data
    )
    return item


def create_item():
    conn = boto.connect_dynamodb()
    table = conn.get_table(DYNAMODB_TABLE)
    item = create_random_item(table)
    item.put()


def create_items():
    num = 1000
    chuck = 25

    for i in range(num / chuck):
        print 'Putting %d items into the table...' % chuck,
        sys.stdout.flush()
        jobs = [gevent.spawn(create_item) for j in range(chuck)]
        gevent.joinall(jobs)
        print 'Complete.'


def create_table(conn):
    table_schema = conn.create_schema(
        hash_key_name='key',
        hash_key_proto_value='S'
    )
    table = conn.create_table(
            name=DYNAMODB_TABLE,
            schema=table_schema,
            read_units=10,
            write_units=10
    )
    print 'Creating the %s table... ' % DYNAMODB_TABLE,
    sys.stdout.flush()
    while True:
        table = conn.get_table(DYNAMODB_TABLE)
        if table.status == 'ACTIVE':
            print 'Complete.'
            break
        time.sleep(1)
    return table


def scan_table(conn):
    table = conn.get_table(DYNAMODB_TABLE)
    print 'Scanning the table to count items... ',
    items = table.scan()
    print 'Complete.'

    count = 0
    for item in items:
        count = count + 1

    print 'There are %d items in the table.' % count


def delete_table(conn):
    print 'Deleting the table ... ',
    sys.stdout.flush()
    table = conn.get_table(DYNAMODB_TABLE)
    conn.delete_table(table)
    while True:
        if DYNAMODB_TABLE not in conn.list_tables():
            break
        time.sleep(1)

    print 'Complete.'


def main():
    conn = boto.connect_dynamodb()
    create_table(conn)
    create_items()
    scan_table(conn)
    delete_table(conn)

if __name__ == '__main__':
    main()
