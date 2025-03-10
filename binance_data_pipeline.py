import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Binance API Endpoint
url = "https://api.binance.com/api/v3/ticker/price"

# Step 1: Fetch live crypto data from Binance
response = requests.get(url)
data = response.json()

# Step 2: Convert JSON response to Pandas DataFrame
df = pd.DataFrame(data)
df["price"] = df["price"].astype(float)  # Convert price to float
df["updated_at"] = datetime.now()  # Add timestamp for tracking updates

# Step 3: Connect to PostgreSQL
db_user = "postgres"
db_password = "P%40s"  
db_host = "localhost"
db_name = "binance_db"

# Create the SQLAlchemy connection
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}")

# Step 4: Store data in PostgreSQL (Overwrite existing table)
df.to_sql("crypto_prices", con=engine, if_exists="replace", index=False)

print("Data updated successfully!")