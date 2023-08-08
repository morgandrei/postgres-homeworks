"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import os
import csv

password = os.getenv("PASSQL")

conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password=password
)
try:
    with conn:
        with conn.cursor() as cur:
            with open("north_data\customers_data.csv", encoding='UTF-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    customer_id = row['customer_id']
                    company_name = row['company_name']
                    contact_name = row['contact_name']
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", (customer_id, company_name, contact_name))
                    conn.commit()


            with open("north_data\employees_data.csv", encoding='UTF-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    employee_id = int(row['employee_id'])
                    first_name = row['first_name']
                    last_name = row['last_name']
                    title = row['title']
                    birth_date = row['birth_date']
                    notes = row['notes']
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", (employee_id, first_name, last_name, title, birth_date, notes))
                    conn.commit()

            with open("north_data\orders_data.csv", encoding='UTF-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    order_id = row['order_id']
                    customer_id = row['customer_id']
                    employee_id = row['employee_id']
                    order_date = row['order_date']
                    ship_city = row['ship_city']
                    cur.execute("INSERT INTO orders VALUES(%s, %s, %s, %s, %s)", (order_id, customer_id, employee_id, order_date, ship_city))
                    conn.commit()


finally:
    conn.close()
