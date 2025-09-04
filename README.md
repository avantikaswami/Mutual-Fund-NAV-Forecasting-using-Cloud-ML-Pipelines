# 📈 Mutual Fund NAV Forecasting using Cloud & ML Pipelines

## 🔹 Overview

This project demonstrates an **end-to-end machine learning pipeline** for forecasting **Mutual Fund Net Asset Value (NAV)** using **cloud services and time-series models**. It integrates **data ingestion, preprocessing, model training, orchestration, storage, and visualization** into a fully automated workflow deployed on **Microsoft Azure**.

## 🔹 Key Features

* **Automated Data Ingestion** – Daily NAV data fetched from **mfapi.in** and **Kuvera APIs** using Python scripts.
* **Orchestration with Apache Airflow** – Scheduled ETL and ML tasks for seamless automation.
* **Secure Cloud Storage** – Raw data stored in **Azure Blob Storage** with credentials managed via **Azure Key Vault**.
* **ETL & Feature Engineering** – Data preprocessing, cleaning, and feature creation using **Azure Data Factory** and **Databricks**.
* **Model Training & Forecasting** – Time-series models implemented:
  * Linear Regression
  * XGBoost
  * ARIMA
  * LSTM (TensorFlow/Keras)
* **Data Warehouse Integration** – Processed and transformed data stored in **Azure SQL Database**.
* **Visualization & Insights** – Power BI dashboards for NAV trends, fund comparison, and predictive insights.

## 🔹 Tech Stack

* **Programming & ML:** Python, Pandas, NumPy, Scikit-learn, XGBoost, TensorFlow/Keras
* **Workflow & Orchestration:** Apache Airflow
* **Cloud Services (Azure):** Blob Storage, Key Vault, Data Factory, Databricks, SQL Database
* **Visualization:** Power BI
* **Duration:** 1 Month

## 🔹 Architecture Workflow

1. **Data Ingestion**

   * Fetch NAV data daily via API calls (mfapi.in, Kuvera).
   * Store raw data in **Azure Blob Storage**.

2. **Orchestration**

   * **Apache Airflow DAGs** manage ingestion, preprocessing, model training, and reporting.

3. **ETL & Feature Engineering**

   * **Azure Data Factory** pipelines for cleaning and transformation.
   * **Databricks notebooks** for feature engineering (rolling averages, volatility, returns).

4. **Model Training & Prediction**

   * Train and evaluate ML models (Linear Regression, XGBoost, ARIMA, LSTM).
   * Save best-performing model for predictions.

5. **Storage & Visualization**

   * Store processed and forecasted NAV in **Azure SQL Database**.
   * Build **Power BI dashboards** for fund comparison, risk analysis, and prediction trends.

## 🔹 Results

* Automated **daily NAV forecasting pipeline**.
* Achieved accurate time-series predictions with **XGBoost & LSTM outperforming ARIMA**.
* Power BI dashboards enabled **interactive investment insights**.
