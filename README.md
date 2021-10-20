# Redrive SQS

Usage:

```
usage: redrive.py [-h] -s SRC -t TGT -f FILTER [--retries RETRIES]

optional arguments:
  -h, --help            show this help message and exit
  -s SRC, --src SRC     Name of source SQS
  -t TGT, --tgt TGT     Name of targeted SQS
  -f FILTER, --filter FILTER
                        Filter on the payload
  --retries RETRIES     Number of retries before stopping to redrive
```

Example:

```bash
python redrive.py -s <source-queue-name> -t <target-queue-name> -f "Something In The Message Body" --retries 2
```


Requires python3 and boto3:

```bash
pip3 install boto3
```
