from flask import Flask
import psycopg2
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from config import DATABASE_CONFIG, COINMARKETCAP_API_KEY, COINMARKETCAP_API_URL

app = Flask(__name__)

# Connect to PostgreSQL database
def connect_db():
    try:
        connection = psycopg2.connect(**DATABASE_CONFIG)
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
    

# Create the table if it doesn't exist
def create_table():
    connection = connect_db()
    if connection:
        cursor = connection.cursor()
        create_table_query = """
            CREATE TABLE IF NOT EXISTS cryptocurrency_data (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                symbol VARCHAR(10),
                price_usd NUMERIC,
                market_cap_usd NUMERIC,
                volume_24h_usd NUMERIC,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        cursor.execute(create_table_query)
        connection.commit()
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
        for crypto in data:
            insert_query = """
                INSERT INTO cryptocurrency_data (name, symbol, price_usd, market_cap_usd, volume_24h_usd)
                VALUES (%s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (crypto['name'], crypto['symbol'], crypto['quote']['USD']['price'], crypto['quote']['USD']['market_cap'], crypto['quote']['USD']['volume_24h']))
        connection.commit()
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
    create_table()  # Create table on startup if it doesn't exist
    app.run(debug=True, use_reloader=False)
