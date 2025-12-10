# 📘 Realtime Stock Market Data Pipeline
A complete real-time data engineering pipeline that streams live stock prices, processes them using PySpark, stores analytics in PostgreSQL, and makes them available for reporting tools like Power BI.


Built using the required stack: **Python, Kafka, PySpark, PostgreSQL, Power BI**.

---


# 🔧 Features


* **Python Producer**: Fetches real-time stock data from Alpha Vantage and sends it to Kafka.
* **Kafka**: Serves as the message broker for real-time streaming.
* **PySpark Structured Streaming**: Reads Kafka topics, processes data (moving averages, volume), and writes results to PostgreSQL.
* **PostgreSQL**: Stores both raw and processed market data.
* **Power BI**: Connects to PostgreSQL for visualization and reporting.


---


# 📂 Project Structure


```
realtime_stock_pipeline/
│
├── producer/producer.py        # Python → Kafka ingestion
├── spark/spark_streaming.py    # Kafka → PySpark → PostgreSQL
├── db/schema.sql               # DB tables and indexes
├── docker-compose.yml          # Kafka, Zookeeper, PostgreSQL
├── requirements.txt            # Python dependencies
```

---

# ⚙️ Quick Start

### 1. Start Docker Services


```bash
docker-compose up -d
```


### 2. Apply Database Schema


```bash
psql -h localhost -U postgres -d marketdb -f db/schema.sql
```


### 3. Install Python Dependencies


```bash
pip install -r requirements.txt
```


### 4. Set API Key and Run Producer


```bash
export ALPHA_VANTAGE_KEY=your_api_key_here
python producer/producer.py
```


### 5. Run PySpark Streaming Job


```bash
spark-submit \
 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
 spark/spark_streaming.py
```


---


# 📊 Power BI Connection


1. Open Power BI Desktop → **Get Data** → *PostgreSQL*.
2. Enter connection details:


```
Host: localhost
Database: marketdb
User: postgres
Password: postgres
```


3. Load tables:


* `symbol_metrics`
* `ticks` (optional)


---


# 📝 Notes

* Alpha Vantage has rate limits; streaming frequency should be controlled.
* For production: add retries, monitoring, and Kafka partitions.

