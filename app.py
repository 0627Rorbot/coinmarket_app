from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from pymongo import MongoClient
from datetime import datetime
import requests

app = Flask(__name__)

# mongodb+srv://rorbotjackson0627:rorbot$0627@cluster0.05vwotz.mongodb.net/
# MongoDB connection
client = MongoClient("mongodb+srv://rorbotjackson0627:rorbot$0627@cluster0.05vwotz.mongodb.net/")
db = client["coinmarket"]
collection = db["prices"]

api_key = '88caa134-4040-4476-b79a-73ac5fbb8bef'

# CoinMarketCap API details
url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
headers = {
    "X-CMC_PRO_API_KEY": api_key,
    "Accepts": "application/json"
}
parameters = {
    "start": "1",
    "limit": "500",
    "convert": "USD"
}

# Function to fetch and save data
def fetch_api_data():
    response = requests.get(url, headers=headers, params=parameters)
    data = response.json()

    try:
        if 'data' in data:
            for crypto in data['data']:
                crypto_record = {
                    "name": crypto['name'],
                    "rank": crypto['cmc_rank'],
                    "symbol": crypto['symbol'],
                    "price_usd": crypto['quote']['USD']['price'],
                    "market_cap_usd": crypto['quote']['USD']['market_cap'],
                    "volume_24h_usd": crypto['quote']['USD']['volume_24h'],
                    "last_updated": datetime.now()
                }
                # Insert into MongoDB
                collection.insert_one(crypto_record)

            print("Data inserted successfully into MongoDB")
        else:
            print("Error fetching data:", data)
    except Exception as e:
        print(f"An error occurred: {e}")
        

# Scheduler configuration
scheduler = BackgroundScheduler()
scheduler.add_job(func=fetch_api_data, trigger="interval", minutes=2)

# Start the scheduler immediately
# scheduler.start()


@app.route('/')
def home():
    scheduler.start()
    return jsonify({"message": "Flask app with APScheduler is running!"})

if __name__ == '__main__':
    try:
        app.run(debug=False, use_reloader=False)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()