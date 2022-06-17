from kafka import KafkaConsumer

consumer = KafkaConsumer('FuturesDepthData', auto_offset_reset='earliest', bootstrap_servers= ['localhost:9092'])

for msg in consumer:
    print(msg)

