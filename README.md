# 🚀 Azure Data Engineering Project - Cars Dataset

## **Overview**  
This project demonstrates an **end-to-end Azure Data Engineering pipeline** using **Azure Databricks**, **Azure Data Lake**, **Azure SQL Database**, and **Azure Data Factory**. It follows the **Medallion Architecture** (Bronze, Silver, Gold) and includes incremental data loading, star schema modeling, and **slowly changing dimensions (SCD Type-1)**.  

## **Architecture**  
- **Bronze Layer**: Stores raw data from various sources.  
- **Silver Layer**: Cleansed and transformed data stored in Parquet format.  
- **Gold Layer**: Aggregated and modeled data optimized for analytics.  

## **Technologies Used**  
✅ **Azure Data Lake** - Storage for raw and processed data  
✅ **Azure Databricks** - Data transformation using PySpark  
✅ **Azure Data Factory** - Orchestrating ETL pipelines  
✅ **Azure SQL Database** - Storing processed data for reporting  
✅ **Unity Catalog** - Metadata and governance  

## **Project Workflow**  
1️⃣ **Load Raw Data**  
   - The raw **Cars Sales dataset** is ingested into the **Bronze layer**.  

2️⃣ **Silver Layer Processing**  
   - Cleansing, transformations, and derived columns (e.g., `Revenue per Unit`).  
   - Scripts: [`Silver_Notebook.py`](./Silver_Notebook.py)  

3️⃣ **Gold Layer (Dimensional Modeling)**  
   - **Dimensional Tables**:  
     - [`Gold_Dim_Branch.py`](./Gold_Dim_Branch.py)  
     - [`Gold_Dim_Dealer.py`](./Gold_Dim_Dealer.py)  
     - [`Gold_Dim_Date.py`](./Gold_Dim_Date.py)  
     - [`Gold_Dim_Model.py`](./Gold_Dim_Model.py)  
   - **Fact Table**:  
     - [`Gold_fact_sales.py`](./Gold_fact_sales.py)  

4️⃣ **Incremental Data Processing**  
   - Implemented **Change Data Capture (CDC)** using **Databricks Delta** for only processing new data.  

5️⃣ **Final Data Storage**  
   - Processed data is stored in **Azure SQL Database** for reporting & analytics.  

## **Getting Started**  
### Prerequisites  
- **Azure Subscription** with Data Lake, Databricks, and Data Factory access  
- **Python 3.x** and **PySpark**  
- **Databricks CLI** (for running notebooks)  

### Steps to Run  
1. **Clone this repo**  
   ```bash
   git clone https://github.com/yourusername/azure-data-engineering.git
   cd azure-data-engineering
   ```  
2. **Upload Notebooks to Databricks**  
3. **Run the ETL Pipeline** step by step  
4. **Validate the Data in Azure SQL Database**  

## **Results & Insights**  
- Created a **star schema** with fact and dimension tables.  
- Improved performance using **incremental loads**.  
- Enabled **real-time analytics** for sales data.  

## **Contributors**  
👤 **Avinash Patel**  
📧 [avinashpatel00x@gmail.com](mailto:avinashpatel00x@gmail.com)  

---
