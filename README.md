# ⌚ Watches Data Sales Report Automation Using ETL

## 📂 Repository Outline

```
p2-ftds029-hck-m3-Khalif-Coding
│
├── /dags                         # DAG scripts
│   └── P2M3_Khalif_DAG.py         # ETL pipeline DAG
│
├── /data                         # Extracted & processed datasets
│   ├── Khalif_Raw.csv             # Raw data extracted from PostgreSQL
│   └── Data_Clean.csv             # Cleaned dataset for Elasticsearch
│
├── /images                       # Visualizations & documentation
│   ├── 1 Introduction & Objectives.png
│   ├── 2 Conclusion & Business Recommendation.png
│   ├── Plot 1 - Horizontal.png
│   ├── Plot 2 - Vertical.png
│   ├── Plot 3 - Pie.png
│   ├── Plot 4 - Table.png
│   ├── Plot 5 - Area.png
│   └── Plot 6 - Heatmap.png
│
├── /logs                         # Airflow DAG logs
├── /plugins                      # (empty) placeholder
├── /postgres_data                # PostgreSQL container data
│
├── .env                          # Database configuration
├── airflow.yaml                  # Docker container configuration
├── description.md                 # Project description
├── P2M3_Khalif_Conceptual         # Conceptual Q&A
├── P2M3_Khalif_DAG_Graph.png      # DAG workflow visualization
├── P2M3_Khalif_DDL.txt            # SQL schema script
├── P2M3_Khalif_GX.ipynb           # Great Expectations notebook
├── P2M3_Khalif_GX_Result.png      # GX validation result
├── README.md                      # Documentation
```

---

## 📌 Problem Background
Pasar jam tangan mewah merupakan industri bernilai miliaran dolar yang sangat bergantung pada **brand equity**, **kualitas produk**, serta **strategi pemasaran dan distribusi** yang presisi.  

Namun, perusahaan sering menghadapi tantangan berikut dalam memaksimalkan penjualan dan profitabilitas:
- Segmentasi konsumen yang kurang optimal  
- Manajemen stok yang tidak efisien  
- Minimnya pemanfaatan data penjualan aktual  

---

## 🎯 Project Output
- DAG Script untuk proses **ETL**  
- Visualisasi & analisis dari data hasil ETL  
- Validasi data menggunakan **Great Expectations**  

---

## 📊 Data
- **Link Dataset:** [Kaggle - Watch Prices Dataset](https://www.kaggle.com/datasets/beridzeg45/watch-prices-dataset)  
- **Ukuran:** 15.000 rows, 10 columns  
- **Tipe Data:** 8 object, 2 float  

---

## ⚙️ Method
- ETL Pipeline dengan **Airflow**  
- Data Validation dengan **Great Expectations**  
- Visualization menggunakan **Kibana**  

---

## 🛠️ Tech Stacks

### 🔹 Languages
- Python  
- Pandas  
- SQL  
- YAML  

### 🔹 Tools
- Visual Studio Code  
- Docker  
- PostgreSQL  
- Airflow Webserver  
- Elasticsearch  
- Kibana  

### 🔹 Libraries
```python
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from elasticsearch import Elasticsearch

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
```

📖 References
- [Fortune Business Insights - Luxury Watch Market](https://www.fortunebusinessinsights.com/luxury-watch-market-104567)
- [Deloitte - Global Powers of Luxury Goods](https://www2.deloitte.com/global/en/pages/consumer-industrial-products/articles/gx-cip-global-powers-of-luxury-goods.html)
- [McKinsey - State of Fashion](https://www.mckinsey.com/industries/retail/our-insights/state-of-fashion)
