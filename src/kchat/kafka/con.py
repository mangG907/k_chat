from kafka import KafkaConsumer
from json import loads

OFFSET_FILE = 'consumer_offset.txt' #몇번째까지 읽었는가

def save_offset(offset): #저장하는 곳
    with open(OFFSET_FILE, 'w') as f: #w = write
        f.write(str(offset))

def read_offset(): #저장했으니 읽어야지요
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as f:
            return int(f.read().strip()) #공백 없애기
    return None
saved_offset = read_offset()
consumer = KafkaConsumer(
        "topic1",
        bootstrap_servers=['localhost:9092'], #
        value_deserializer=lambda x: loads(x.decode('utf-8')), 
        consumer_timeout_ms=5000, 
        auto_offset_reset='earliest' if saved_offset is None else'none'
        group_id="fbi",
        enable_auto_commit=True,
)

print('[Start] get consumer')
p = TopicPartition('topic1', 0)
consumer.assign([p])
if saved_offset is not None:
    consumer.seek(p, saved_offset)
else:
    consumer.seek_to_beginning(p)#내가 ㄹㅇ찾아본다.

for m in consumer:
    print(f"offset = {m.offset}, value = {m.value}")
    save_offset(m.offset + 1)
print('[End] get consumer')


