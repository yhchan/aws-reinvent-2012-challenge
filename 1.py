#/usr/bin/env python

from PIL import Image
import boto
import os
import time

PHOTO_FILE = '1.jpg'
S3_BUCKET = 'awschallenge1'
DYNAMODB_TABLE = S3_BUCKET


def upload():
    c = boto.connect_s3()
    b = c.create_bucket(S3_BUCKET)
    from boto.s3.key import Key
    k = Key(b)
    k.key = PHOTO_FILE
    k.set_contents_from_filename(PHOTO_FILE)


def write_to_dynamodb():
    jpgfile = Image.open(PHOTO_FILE)
    (bits, (height, width), mode) = (jpgfile.bits, jpgfile.size, jpgfile.mode)
    create_time = time.ctime(os.path.getctime(PHOTO_FILE))
    print bits, height, width, mode
    print create_time

    conn = boto.connect_dynamodb()
    """
    table_schema = conn.create_schema(
        hash_key_name='S3Key',
        hash_key_proto_value='S'
    )
    table = conn.create_table(
            name=DYNAMODB_TABLE,
            schema=table_schema,
            read_units=1,
            write_units=1
    )
    """
    table = conn.get_table(DYNAMODB_TABLE)
    item_data = {
        'bits': bits,
        'height': height,
        'width': width,
        'mode': mode,
        'create_time': create_time
    }
    item = table.new_item(
        hash_key=PHOTO_FILE,
        attrs=item_data
    )
    item.put()


def main():
    upload()
    write_to_dynamodb()
    pass

if __name__ == '__main__':
    main()
