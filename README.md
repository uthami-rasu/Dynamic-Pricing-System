
# Dynamic Pricing System for E-Commerce Products  

## Overview  
This project is a **Dynamic Pricing System** designed for e-commerce platforms. The system dynamically adjusts product prices in real-time based on data such as competitor pricing, stock levels, and external events (e.g., holidays). It ensures timely and informed price adjustments, helping businesses maintain a competitive edge while optimizing profits.  

## Objective  
- To build an end-to-end system that processes data in real-time and dynamically adjusts product prices.  
- To automate and orchestrate workflows for seamless execution.  
- To visualize pricing trends and actionable insights on an interactive dashboard.  

## Tools and Technologies  
- **Apache Kafka**: Real-time data ingestion and message brokering.  
- **Apache Spark Streaming**: Processing and transforming data streams in real-time.  
- **MongoDB**: Storing processed pricing data and maintaining historical records.  
- **Apache Airflow**: Workflow orchestration and automation.  
- **Power BI**: Data visualization and reporting.  
---
## System Workflow Diagram
![ER Diagram](https://raw.githubusercontent.com/uthami-rasu/Dynamic-Pricing-System/refs/heads/main/Diagram/System%20Architecture.png)


---

## System Workflow Steps
The system follows these steps to process and manage dynamic pricing:  

### Step 1: Data Generation and Ingestion  
- A **Kafka Producer** simulates product data, including product ID, competitor prices, stock levels, and special event indicators.  
- The data is published to a Kafka topic (`products`).  

### Step 2: Real-time Data Processing  
- **Apache Spark Streaming** consumes data from the Kafka topic in real-time.  
- The streaming data is cleaned, transformed, and enriched with computed fields (e.g., adjusted price, metadata).  

### Step 3: Data Storage  
- The processed pricing data is stored in **MongoDB**, ensuring a complete historical record of price changes for auditing and analysis.  

### Step 4: Workflow Orchestration  
- **Apache Airflow** orchestrates the pipeline, automating tasks such as data ingestion, real-time processing, and storage.  
- Airflow ensures smooth end-to-end execution with built-in monitoring and alerts.  

### Step 5: Data Visualization  
- Processed data is consumed by **Power BI**, where a dashboard displays:  
  - Real-time pricing trends.  
  - Competitor price comparisons. 

---

## Outcome and Dashboard  
### Outcome  
- A fully functional system capable of real-time data processing and dynamic pricing.  
- Historical data stored in MongoDB for insights and audits.  
- Interactive dashboards that empower stakeholders to make data-driven decisions.  


### Dashboard Features  
- **Real-time Pricing Trends**: Visualize current prices and trends.  
- **Competitor Price Comparison**: Compare your prices against competitors in real-time.  
- **Stock Analysis**: Monitor stock levels and their impact on pricing.  
- **Event Analysis**: Understand pricing dynamics during holidays and promotions.  

![ER Diagram](https://raw.githubusercontent.com/uthami-rasu/Dynamic-Pricing-System/refs/heads/main/Diagram/Insights.png)

---

## Key Learnings  
- Enhanced understanding of **Apache Kafka** and its role in real-time data ingestion.  
- Developed expertise in **Spark Streaming** for real-time data transformation.  
- Learned to design and query **MongoDB** for managing dynamic data.  
- Leveraged **Apache Airflow** to automate and orchestrate data workflows efficiently.  
- Built interactive dashboards in **Power BI** for actionable insights.  

--- 
## Contact
For questions or collaboration, please reach out at uthamirasuv@gmail.com.
