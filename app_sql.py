from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from config import COINMARKETCAP_API_KEY, COINMARKETCAP_API_URL
import requests
import psycopg2

app = Flask(__name__)

# Connect to PostgreSQL database
def connect_db():
    try:
        DATABASE_URL = "postgresql://rorbotjackson0627:aNLxsLzu32IXNE42dO1V9ay3p5DdOwvU@dpg-cr683stsvqrc73c7fm10-a.oregon-postgres.render.com/coinmarket"
        connection = psycopg2.connect(DATABASE_URL)
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

# Create the tables if they don't exist
def create_tables():
    connection = connect_db()
    if connection:
        cursor = connection.cursor()
        try:
            # Create cryptocurrencies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cryptocurrencies (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) UNIQUE,
                    symbol VARCHAR(10) UNIQUE
                );
            """)
            # Create cryptocurrency_prices table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cryptocurrency_prices (
                    id SERIAL PRIMARY KEY,
                    cryptocurrency_id INTEGER REFERENCES cryptocurrencies(id),
                    price_usd NUMERIC,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            connection.commit()
            print("Tables created or already exist.")
        except Exception as e:
            print(f"Error creating tables: {e}")
        finally:
            cursor.close()
            connection.close()

# Fetch data from CoinMarketCap API
def fetch_coinmarket_data():
    headers = {
        'X-CMC_PRO_API_KEY': COINMARKETCAP_API_KEY,
        'Accepts': 'application/json'
    }
    
    parameters = {
        "start": "1",
        "limit": "500",
        "convert": "USD"
    }

    response = requests.get(COINMARKETCAP_API_URL, headers=headers, params=parameters)
    if response.status_code == 200:
        data = response.json()
        return data['data']
    else:
        print(f"Failed to fetch data from CoinMarketCap API: {response.status_code}")
        return None
    
# Save data to PostgreSQL database
def save_data_to_db(data):
    connection = connect_db()
    if connection:
        cursor = connection.cursor()
        try:
            for crypto in data:
                cursor.execute("""
                    INSERT INTO cryptocurrencies (name, symbol)
                    VALUES (%s, %s)
                    ON CONFLICT (name, symbol) DO NOTHING;
                """, (crypto['name'], crypto['symbol']))
                cursor.execute("SELECT id FROM cryptocurrencies WHERE name = %s;", (crypto['name'],))
                cryptocurrency_id = cursor.fetchone()[0]
                cursor.execute("""
                    INSERT INTO cryptocurrency_prices (cryptocurrency_id, price_usd)
                    VALUES (%s, %s);
                """, (cryptocurrency_id, crypto['quote']['USD']['price']))
            connection.commit()
            print("Data inserted into the database successfully.")
        except Exception as e:
            print(f"Error inserting data into the database: {e}")
        finally:
            cursor.close()
            connection.close()


# Scheduled task to fetch and save data
def scheduled_task():
    print("Fetching data from CoinMarketCap...")
    data = fetch_coinmarket_data()
    if data:
        save_data_to_db(data)
        print("Data saved to PostgreSQL.")

# Start the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_task, 'interval', minutes=2)
scheduler.start()

@app.route('/')
def index():
    return "CoinMarketCap Data Fetcher is running."

if __name__ == "__main__":
    create_tables()  # Create table on startup if it doesn't exist
    app.run(debug=True, use_reloader=False, port=8080)
