---Uber & Fare Data Streaming Simulation on Microsoft Fabric---

**A simulated real-time data streaming project using Microsoft Fabric and PySpark, inspired by Uber ride and fare data.**

## Project Overview

This project simulates Uber ride and fare data streams using CSV files stored in Google Drive, mimicking real-time streaming scenarios. Data is ingested, cleaned, and transformed in a **Bronze → Silver → Gold** layered architecture using Microsoft Fabric, PySpark, and Lakehouse storage. The final Gold layer is structured for analytics with Power BI.
## 🛠️ Tools & Technologies

This project integrates multiple services from **Microsoft Fabric** along with **Python-based utilities** to simulate, process, and analyze streaming data.

### Microsoft Fabric Services
- **Dataflow Gen2** → Data transformation and pipeline orchestration  
- **Pipelines** → Automated scheduling and orchestration of Bronze → Silver → Gold workflows  
- **Activator** → Trigger-based execution of data pipelines  
- **Eventstream** → Real-time event capture and processing  
- **Lakehouse** → Centralized storage for raw and transformed data  
- **Warehouse** → Structured analytical storage for Fact/Dim tables  
- **Monitoring** → Track pipeline runs, errors, and data flow performance  
- **KQL Database** → Querying, log analysis, and monitoring insights  

### Python Libraries
- **gdown** → Download files from Google Drive  
- **requests** → API calls and HTTP communication  
- **pytz** → Timezone handling for scheduling  
- **azure-servicebus** → Publish/consume messages with Azure Service Bus  
- **asyncio** → Asynchronous simulation of streaming data  
- **os, glob, io, zipfile, csv, json** → File management and data parsing  
- **datetime** → Time-based scheduling and event simulation  

### Analytics & Visualization
- **Power BI** → Dashboards and reports for operational, financial, and predictive insights  


---

## Bronze Layer

- **Source:** 6 CSV files in Google Drive (3 files about fares, 3 about Uber rides).  
- **Simulation:** Python code reads these files at scheduled intervals to mimic streaming data.  
- **Destination:** Data is stored in Lakehouse as **two separate raw tables**: `raw_data` (ride info) and `raw_fare` (fare info).  
- **Objective:** Capture raw streaming-like data for initial ingestion.

---

## Silver Layer

- **Data Cleaning:**  
  - Remove duplicates  
  - Handle null values  
  - Correct date formats  
- **Merging:** The two raw tables (`raw_data` and `raw_fare`) are merged into a single cleaned table: `cleaned_silver_trips`.  
- **Objective:** Produce a clean, consistent dataset ready for analytics.

---

## Gold Layer

- **Transformation:**  
  - Using **Data Flow Gen2**, create **Fact** and **Dimension** tables based on the cleaned Silver layer.  
  - Implement a **Star Schema** for efficient analytics.  
- **Destination:** Fabric Warehouse, optimized for Power BI dashboards and reports.  
- **Objective:** Make high-quality, query-ready data available for business analysis and visualization.

---

## Technologies Used

- **Microsoft Fabric** (Data Factory, Lakehouse, Warehouse)  
- **PySpark** for ETL and transformations  
- **Python** for streaming simulation from Google Drive CSV files  
- **Power BI** for visualization and analytics  

---

## Power BI Analysis

After the Gold layer, the data is fully **cleaned, transformed, and modeled** into a **Star Schema** with Fact and Dimension tables. At this stage, the dataset becomes reliable, consistent, and ready for business intelligence.

Using **Power BI**, we connected directly to the Fabric Warehouse and built dashboards that provide:

-  **Operational Insights:** Daily/weekly ride volumes, fare trends, and demand fluctuations.  
-  **Business Metrics:** Driver activity, trip durations, peak hours, and location-based ride patterns.  
-  **Financial Analysis:** Revenue distribution, average fare per trip, and seasonal profitability trends.  
-  **Future Planning:** By analyzing historical patterns, the company can forecast demand, optimize driver allocation, and improve pricing strategies.  

This layer demonstrates how **raw data → cleaned data → business-ready insights** can empower decision-makers.  
With the integration of Microsoft Fabric and Power BI, stakeholders gain a **single source of truth** for analytics, enabling strategic planning and evidence-based decision-making for future growth.
---

## Author

**Eren Karaca**  
- [LinkedIn](https://www.linkedin.com/in/eren-karaca-81748b343/)  
- [GitHub](https://github.com/erennecatikaraca)
                                                                        

**MIT License**


