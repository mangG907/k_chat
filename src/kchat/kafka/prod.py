from kafka import KafkaProducer
import time
import json
from tqdm import tqdm

producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer = lambda x : json.dumps(x).encode('utf-8')
)
#json 형식을 읽을때는 직렬?로 읽어줘야함

start = time.time() # 시작 시간 보려고

for i in tqdm(range(10)):
    data = {'str' : 'value' + str(i)}
    # send('지정 토픽명', value = 메시지값)
    producer.send('topic1', value = data)
    time.sleep(1)
    # 종료 구문
    producer.flush()

end = time.time() # 끝나는 시간 확인 
print(f"DONE : {end - start}")
