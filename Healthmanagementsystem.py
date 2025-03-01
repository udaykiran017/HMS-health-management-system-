import random
import json
from pyspark.sql import SparkSession
from pymongo import MongoClient


session = SparkSession.builder.master("local[*]").appName("HealthDataAnalysis").getOrCreate()


def create_records(count=10000):
    return [
        {
            "record_id": i,
            "blood_pressure": random.randint(90, 180),
            "glucose": round(random.uniform(70, 250), 1),
            "lipid_levels": random.randint(100, 300),
            "hb_count": round(random.uniform(9, 16), 1)
        }
        for i in range(count)
    ]

data = create_records()
with open("health_records.json", "w") as file:
    json.dump(data, file)


dataframe = session.read.option("multiline", "true").json("health_records.json")
dataframe.createOrReplaceTempView("medical_records")


metrics = session.sql("""
    SELECT 
        AVG(blood_pressure) as avg_bp, 
        AVG(glucose) as avg_glucose, 
        AVG(lipid_levels) as avg_cholesterol, 
        AVG(hb_count) as avg_hb 
    FROM medical_records
""")
metrics.show()


try:
    db_client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
    db_client.server_info()
    health_db = db_client['medical_db']
    feedback_table = health_db['patient_feedback']
    print("Connected to database successfully.")
except Exception as err:
    print("Database connection failed:", err)


def log_feedback(entry_id, remark):
    if db_client:
        feedback_table.insert_one({"record_id": entry_id, "response": remark})
        print(f"Feedback logged for Record {entry_id}: {remark}")


log_feedback(150, "Positive response")
log_feedback(275, "Needs follow-up")
