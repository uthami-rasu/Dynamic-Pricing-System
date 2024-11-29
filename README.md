
# **Dynamic Pricing System for E-Commerce**

## **Overview**

This project implements a **Dynamic Pricing System** for e-commerce products using Apache Kafka, Apache Spark, and MongoDB. The system listens to product price data from Kafka, processes it in real-time with Spark Streaming, and stores the processed data (including pricing adjustments) in MongoDB. It also includes functionality for generating fake product data for testing and simulating real-time price adjustments based on various factors like competitor price, stock levels, and holidays.

---

## **Technologies Used**

- **Apache Kafka**: For message brokering.
- **Apache Spark**: For real-time stream processing (Spark Streaming).
- **MongoDB**: For storing the processed pricing data.
- **Python**: Programming language for building the application.

---

## **System Architecture**

1. **Kafka Producer**: Generates fake product data and sends it to the `prouducts_topic` Kafka topic.
2. **Kafka Consumer**: Consumes messages from the Kafka topic in real-time and processes them using Spark Streaming.
3. **Spark Streaming**: Processes data in real-time, performs various transformations (e.g., calculating new price, checking holidays/weekends), and stores the data in MongoDB.
4. **MongoDB**: Stores the processed product data including pricing history and versioning.



## Spark & Kafka Streaming Workflow 

![Workflow](https://raw.githubusercontent.com/uthami-rasu/Spark-Stream-Processing/refs/heads/main/utils/Workflow%20Diagram.png)

---

## **Features**

- **Kafka Producer**: Generates fake product data with attributes such as product ID, base price, competitor price, stock level, and sales rate.
- **Dynamic Pricing Logic**: Adjusts product pricing based on competitor price, stock level, sales rate, and if it's a holiday or weekend.
- **Versioning**: Each product's pricing history is versioned and updated when new data is processed.
- **Error Handling and Logging**: Logs key actions and handles exceptions throughout the process.

---

## **Setup and Installation**

### Prerequisites

- **Python 3.8+**
- **Apache Kafka**: For message brokering. Ensure Kafka is up and running.
- **Apache Spark**: Ensure Spark is properly set up with the necessary connectors (Kafka and MongoDB).
- **MongoDB**: Ensure MongoDB is running and accessible.
- **Python Libraries**: All the necessary libraries are listed in `requirements.txt`.

### Installation Steps

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-repo/dynamic-pricing.git
   cd dynamic-pricing
   ```

2. **Create a virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**:
   Create a `.env` file in the root directory with the following content:
   ```dotenv
   KAFKA_SERVERS=your_kafka_server
   MONGO_URI=your_mongo_uri
   LAMBDA_ENDPOINT=your_lambda_endpoint
   ```
---



