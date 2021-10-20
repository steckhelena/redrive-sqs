"""
This script is used to redrive message in (src) queue to (tgt) queue

The solution is to set the Target Queue as the Source Queue's Dead Letter Queue.
Also set Source Queue's redrive policy, Maximum Receives to 1. 
Also set Source Queue's VisibilityTimeout to 5 seconds (a small period)
Then read data from the Source Queue.

Source Queue's Redrive Policy will copy the message to the Target Queue.
"""
import argparse
import json
import boto3

sqs = boto3.client("sqs")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src", required=True, help="Name of source SQS")
    parser.add_argument(
        "-t", "--tgt", required=True, help="Name of targeted SQS"
    )
    parser.add_argument(
        "-f", "--filter", required=True, help="Filter on the payload"
    )
    parser.add_argument(
        "--retries",
        required=False,
        default=0,
        type=int,
        help="Number of retries before stopping to redrive",
    )

    args = parser.parse_args()
    return args


def dump_as_json(obj):
    print(json.dumps(obj, indent=4, sort_keys=True))


def verify_queue(queue_name):
    queue_url = sqs.get_queue_url(QueueName=queue_name)
    return True if queue_url.get("QueueUrl") else False


def get_queue_attribute(queue_url):
    queue_attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["All"]
    )["Attributes"]
    dump_as_json(queue_attributes)

    return queue_attributes


def main():
    args = parse_args()
    for q in [args.src, args.tgt]:
        if not verify_queue(q):
            print(f"Cannot find {q} in AWS SQS")

    src_queue_url = sqs.get_queue_url(QueueName=args.src)["QueueUrl"]
    target_queue_url = sqs.get_queue_url(QueueName=args.tgt)["QueueUrl"]

    print("Source Queue:")
    get_queue_attribute(src_queue_url)

    print("Target Queue:")
    get_queue_attribute(target_queue_url)

    # redrive all messages
    num_received = 0
    retries = args.retries
    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=src_queue_url,
                MaxNumberOfMessages=10,
                AttributeNames=["All"],
                WaitTimeSeconds=5,
            )

            messages = resp.get("Messages", [])

            # filter messages to be redriven
            messages = [
                message
                for message in messages
                if args.filter in message["Body"]
            ]

            num_message = len(messages)
            if not num_message:
                if retries > 0:
                    retries -= 1
                    continue
                break

            entries = [
                {
                    "Id": message["MessageId"],
                    "MessageBody": message["Body"],
                }
                for message in messages
            ]
            receipt_handles = [
                {
                    "Id": message["MessageId"],
                    "ReceiptHandle": message["ReceiptHandle"],
                }
                for message in messages
            ]

            sqs.send_message_batch(QueueUrl=target_queue_url, Entries=entries)
            sqs.delete_message_batch(
                QueueUrl=src_queue_url, Entries=receipt_handles
            )

            num_received += num_message
            redriven = [entry["Id"] for entry in entries]

            print("Redriven: ", redriven)
        except Exception as e:
            print(e)
            break
    print(f"Redrive {num_received} messages")

    print("Source Queue:")
    get_queue_attribute(src_queue_url)

    print("Target Queue:")
    get_queue_attribute(target_queue_url)


if __name__ == "__main__":
    main()
