from kafka import KafkaConsumer, KafkaProducer
import json

def score_transaction(tx):
    score = 0
    if tx['amount'] > 4000: score += 5
    hour = int(tx['timestamp'][11:13])
    if hour < 5: score += 2
    if tx['category'] == 'elektronika': score += 2
    return score

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    tx = message.value
    score = score_transaction(tx)
    
    if score >= 3:
        alert_data = {"tx_id": tx['tx_id'], "score": score, "status": "SUSPICIOUS"}
        alert_producer.send('alerts', value=alert_data)
        print(f"ALERT: Transakcja {tx['tx_id']} (Score: {score}) wysłana do tematu 'alerts'")
    else:
        print(f"OK: Transakcja {tx['tx_id']} (Score: {score})")
