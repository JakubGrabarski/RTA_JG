from kafka import KafkaConsumer
import json

def score_transaction(tx):
    score = 0
    rules = []
    
    if tx['amount'] > 4000:
        score += 5
        rules.append("R1")
        
    hour = int(tx['timestamp'][11:13])
    if hour < 5:
        score += 2
        rules.append("R2")
        
    if tx['category'] == 'elektronika':
        score += 2
        rules.append("R3")
        
    return score, rules

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)
    
    print(f"ID: {tx['tx_id']} | Kat: {tx['category']} | Score: {score} | Rules: {rules}")
    if score >= 5:
        print("!!! ALERT !!!")
