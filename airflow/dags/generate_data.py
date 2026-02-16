from airflow.decorators import dag, task
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime,timedelta
@dag(
    dag_id = "data_generator",
    schedule = None,
    start_date = datetime(2026,2,14),
    catchup= False
)
def dataGenerator():
    @task
    def generate_transactions():
        fake = Faker()
        transactions=[]
        for i in range(1000):
            transactions.append({
                'transaction_id': i + 1,
                'customer_id': np.random.randint(1, 201),
                'first_name': fake.first_name(),  
                'last_name': fake.last_name(),    
                'product_id': np.random.randint(1, 101), 
                'product_name': fake.word(),       
            'cost': round(np.random.uniform(5, 500), 2), 
            'date': fake.date_time_between(start_date='-30d', end_date='now')
            })
        df = pd.DataFrame(transactions)
        df.to_csv('/opt/airflow/dags/transactions.csv', index=False)
        return '/opt/airflow/dags/transactions.csv'
    generate_transactions()
dataGenerator()


    
    
