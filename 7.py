#/usr/bin/env python

import boto.sqs
import boto.sns
import boto.exception

QUEUE_NAME = 'CodeChallange'
TOPIC_NAME = 'CodeChallange'

REGION = 'us-west-2'
MSG = 'hi'


def main():
    # Create Queue
    conn_q = boto.sqs.connect_to_region(REGION)
    queue = conn_q.create_queue(QUEUE_NAME)

    # Create Topic
    conn_n = boto.sns.connect_to_region(REGION)
    topic = conn_n.create_topic(TOPIC_NAME)

    resp = dict(topic)
    topic_arn = resp['CreateTopicResponse']['CreateTopicResult']['TopicArn']

    # Do Subscription
    try:
        conn_n.subscribe_sqs_queue(topic_arn, queue)
    except boto.exception.SQSError:
        pass

    # Publish
    conn_n.publish(topic_arn, MSG)

    # Poll
    while True:
        msgs = conn_q.receive_message(queue)
        if len(msgs) > 0:
            print msgs[0].get_body()
            break


if __name__ == '__main__':
    main()
