from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sklepy = ['Warszawa', 'Kraków', 'Gdańsk', 'Wrocław']
kategorie = ['elektronika', 'odzież', 'żywność', 'książki']

def generate_transaction():
    losowanie = random.random()
    
    if losowanie <= 0.05:
        amount = round(random.uniform(3001.0, 5000.0), 2)
        category = 'elektronika'
        hour = random.randint(0, 5)
    else:
        amount = round(random.uniform(5.0, 3000.0), 2)
        category = random.choice(kategorie)
        hour = random.randint(6, 23)

    return {
        'tx_id': f'TX{random.randint(1000, 9999)}',
        'user_id': f'user_{random.randint(1, 20):02d}',
        'amount': amount,
        'store': random.choice(sklepy),
        'category': category,
        'hour': hour,
        'timestamp': datetime.now().isoformat()
    }

for i in range(100):
    tx = generate_transaction()
    producer.send('transactions', value=tx)
    print(f"[{i+1}] {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | Kat: {tx['category']} | Godz: {tx['hour']}")
    time.sleep(0.5)

producer.flush()
producer.close()
