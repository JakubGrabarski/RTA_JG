from fastapi import FastAPI
from pydantic import BaseModel
import pickle, numpy as np

app = FastAPI(title="Fraud Detection API")
model = pickle.load(open('fraud_model.pkl', 'rb'))

class Transaction(BaseModel):
    amount: float
    hour: int
    is_electronics: int
    tx_per_day: int

@app.get("/health")
async def health_check():
    return {"status": "ok", "model_loaded": True}

@app.post("/score")
async def score_transaction(td: Transaction):
    data = np.array([[td.amount, td.hour, td.is_electronics, td.tx_per_day]])
    prediction = model.predict(data)[0]
    probability = model.predict_proba(data)[0][1]
    return {
        "is_fraud": bool(prediction),
        "fraud_probability": float(probability)
    }
