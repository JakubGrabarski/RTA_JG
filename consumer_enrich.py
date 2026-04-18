from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='enrich-group-v1', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Rozpoczynam analizę poziomu ryzyka...")

for message in consumer:
    tx = message.value
    
    
    if tx['amount'] > 3000:
        risk_level = "HIGH"
    elif tx['amount'] > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
    
    print(f"ID: {tx['tx_id']} | Kwota: {tx['amount']:.2f} | RYZYKO: {risk_level} | Kat: {tx['category']}")
