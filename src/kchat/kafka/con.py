from kafka import KafkaConsumer
from json import loads

OFFSET_FILE = 'consumer_offset.txt'

def save_offset(offset):
    with open(OFFSET_FILE, 'w') as f:
        f.write(str(offset))

def read_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as f:
            return int(f.read().strip())
    return None

consumer = KafkaConsumer(
        "topic1",
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8')), 
        consumer_timeout_ms=5000,
        auto_offset_reset='earliest' if read_offset() is None else'none'
        group_id="fbi",
        enable_auto_commit=True,
)

print('[Start] get consumer')

saved_offset = read_offset()
if saved_offset is not None:
    p = TopicPartition('topic1', 0)
    consumer.assign([p])
    consumer.seek(p, saved_offset)

for m in consumer:
    print(f"offset = {m.offset}, value = {m.value}")
    save_offset(m.offset + 1)
print('[End] get consumer')


