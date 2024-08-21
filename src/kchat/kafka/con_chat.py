from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
        'chat',
        bootstrap_servers = ["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id='chat_group',
        value_deserializer = lambda x : loads(x.decode('utf-8'))
        )
print("채팅 프로그램 - 메시지 수신")
print("메시지 대기 중 ...")

try: #오류가 나도 돌아가게 함.
    for m in consumer:
        data = m.value
        print(f"[friend] : {data['message']}")
except KeyboardInterrupt:
    print("채팅종료")
finally:
    consumer.close()
