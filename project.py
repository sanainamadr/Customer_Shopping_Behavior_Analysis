import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

# -------------------------
# 1. LOAD DATA
# -------------------------
df = pd.read_csv('customer_shopping_behavior.csv')

print(df.head())
print(df.info())
print(df.describe())
print(df.isnull().sum())
print("")

# -------------------------
# 2. FILL MISSING REVIEW RATINGS BY CATEGORY MEDIAN
# -------------------------
df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(
    lambda x: x.fillna(x.median())
)

print(df.isnull().sum())

# -------------------------
# 3. CLEAN COLUMN NAMES
# -------------------------
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(' ', '_')

# Rename purchase amount
df = df.rename(columns={'purchase_amount_(usd)': 'purchase_amount'})

print(df.columns)

# -------------------------
# 4. CREATE AGE GROUP COLUMN
# -------------------------
labels = ['Young Adult', 'Adult', 'Middle-age', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels=labels)
print(df[['age', 'age_group']].head(10))

# -------------------------
# 5. CREATE PURCHASE FREQUENCY DAYS COLUMN
# -------------------------
frequency_mapping = {
    'Fortnightly': 14,
    'Weekly': 7,
    'Monthly': 30,
    'Quarterly': 90,
    'Bi-weekly': 14,
    'Annually': 365,
    'Every 3 Months': 90
}

df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping)
print(df[['purchase_frequency_days', 'frequency_of_purchases']].head(10))



# -------------------------
# 6. CONNECT TO MYSQL USING mysql.connector (TEST CONNECTION)
# ------------------------
connection = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="Sana@123",
    database="costomer_behavior"
)

cursor = connection.cursor()
cursor.execute("SELECT DATABASE();")
print("Connected to:", cursor.fetchone())
connection.close()

# -------------------------
# 7. CONNECT USING SQLAlchemy to PUSH DATAFRAME TO MYSQL
# -------------------------
# IMPORTANT → encode @ as %40 
password = "Sana%40123"

engine = create_engine(
    f"mysql+pymysql://root:{password}@127.0.0.1/costomer_behavior"
)

# -------------------------
# 8. UPLOAD DATAFRAME TO MYSQL TABLE
# -------------------------
df.to_sql("customer_behavior_data", engine, if_exists="replace", index=False)


