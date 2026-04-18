from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json, requests

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

API_URL = "http://jupyter:8002/score"

for message in consumer:
    tx = message.value
    
    # 1. Wyciągnij cechy
    # Godzinę wyciągamy z timestampa (format ISO: YYYY-MM-DDTHH:MM:SS)
    dt = datetime.fromisoformat(tx['timestamp'])
    
    features = {
        "amount": tx['amount'],
        "hour": dt.hour,
        "is_electronics": 1 if tx['category'] == 'electronics' else 0,
        "tx_per_day": 5
    }
    
    # 2. Odpytaj API
    try:
        response = requests.post(API_URL, json=features)
        result = response.json()
        
        # 3. Jeśli fraud, wyślij alert
        if result.get('is_fraud'):
            alert = {
                "tx_id": tx['tx_id'],
                "status": "ALERT",
                "probability": result['fraud_probability'],
                "details": tx
            }
            alert_producer.send('alerts', alert)
            print(f"!!! ALERT !!! Wykryto oszustwo: {tx['tx_id']} (Prob: {result['fraud_probability']})")
        else:
            print(f"Transakcja OK: {tx['tx_id']}")
            
    except Exception as e:
        print(f"Błąd API: {e}")
