import psycopg2
import random
from datetime import datetime, timedelta

# Подключение к PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="banking_raw",
    user="admin",
    password="admin123"
)
cur = conn.cursor()

# Создаём таблицу
cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id SERIAL PRIMARY KEY,
        user_id INT,
        amount DECIMAL(10,2),
        category VARCHAR(50),
        status VARCHAR(20),
        created_at TIMESTAMP
    )
""")

# Генерируем 100к транзакций
categories = ['transfer', 'payment', 'withdrawal', 'deposit', 'refund']
statuses = ['success', 'failed', 'pending']
weights = [0.85, 0.10, 0.05]

start_date = datetime(2024, 1, 1)
records = []

for i in range(100000):
    records.append((
        random.randint(1, 5000),
        round(random.expovariate(1/150), 2),
        random.choice(categories),
        random.choices(statuses, weights=weights)[0],
        start_date + timedelta(minutes=i*5)
    ))

cur.executemany("""
    INSERT INTO transactions (user_id, amount, category, status, created_at)
    VALUES (%s, %s, %s, %s, %s)
""", records)

conn.commit()
print(f"✅ Загружено {len(records)} транзакций в PostgreSQL")
cur.close()
conn.close()
