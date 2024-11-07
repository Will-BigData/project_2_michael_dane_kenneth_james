
# Project 2: E-Commerce Data Analysis

### Team Members
- Michael Mengistu
- Dane Sperling
- Kenneth Muriel
- James Bushek

### Project Overview
This project involves generating, analyzing, and visualizing an e-commerce dataset to identify trends and insights. The primary phases include data generation, trend input, analysis, and visualization using Python, PySpark, and Power BI.
Trello to manage project tasks: https://trello.com/b/6CAQtTQ9/project-2

### Objectives
1. **Data Generation**: Develop a data generator using Python and PySpark to simulate a dataset with 10,000 to 15,000 records.
2. **Trend Injection**: Introduce specific sales trends and a small percentage (up to 5%) of rogue data for added complexity.
3. **Analysis and Visualization**: Analyze the generated data and visualize it using Power BI.

### Data Generation
- **Structure**: The dataset is modeled on a furniture e-commerce store with:
  - 20 unique customers
  - 22 unique products across 7 categories
  - 5 website names
- **Date Range**: Records span from January 1, 2020, to January 1, 2022.
- **Data Composition**: Approximately 90% of records are randomly generated, with 10% following specific trends.
- **Rogue Data**: 1-2% of records contain intentional inconsistencies to simulate real-world data irregularities.

### Trends
1. **Product Blanket**: Available only in December.
2. **Product Hammock**: Available only from June 1 to August 31.
3. **Bulk Purchase Cities**: High bulk purchases in Chicago, Vancouver, Los Angeles, and Berlin.
4. **Payment Type**: Predominantly internet banking.

### Implementation Details
- **Version 1**:
  - Files: `data_maker.py`, `func.py`, `var.py`
  - `var.py` holds variables, including product, customer, and trend information.
  - `func.py` provides helper functions for data generation.
  - `data_maker.py` creates the Spark session, defines data schema, injects trends, and outputs data to CSV.
  - **Rogue Data**: Injected inconsistencies in `product_id` and `country` columns.
  
- **Version 2**:
  - Improved PySpark integration with `data_maker_2.py` while retaining trend injection and rogue data functionality.

### Key Points for Presentation
- **Data Generation**: Outline of data structure and trend injection.
- **Analysis of Trends**: Insights from trend data.
- **Spark Integration**: Explanation of PySpark functions.
- **Power BI Visuals**: Highlights of visuals and notable Power BI techniques used.

### Notable Power BI Visualizations
Power BI was used to create visuals highlighting trend patterns and outliers, providing an insightful view of sales performance across time, product categories, and customer demographics.

### Project Files
- `data_maker.py`: PySpark script for data generation.
- `func.py`: Helper functions for data generation.
- `var.py`: Variables for customer, product, and trend definitions.
- `data_maker_2.py`: Enhanced PySpark version for data generation.
- `data_cleaner.py`: Script for cleaning and analyzing data.
