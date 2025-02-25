import pandas as pd
import sqlite3
import numpy as np

def extract_data():
    try:
        df_a = pd.read_csv('/content/order_region_a(in).csv')
        df_b = pd.read_csv('/content/order_region_b(in).csv')
        
        df_a['region'] = 'A'
        df_b['region'] = 'B'
        
        combined_df = pd.concat([df_a, df_b], ignore_index=True)
        print("Data extracted successfully.")
        
        return combined_df
    except Exception as e:
        print(f"Error during data extraction: {e}")
        raise

def transform_data(df):
    try:
        df['QuantityOrdered'] = pd.to_numeric(df['QuantityOrdered'], errors='coerce').fillna(0).astype(int)
        df['ItemPrice'] = pd.to_numeric(df['ItemPrice'], errors='coerce').fillna(0)
        df['PromotionDiscount'] = pd.to_numeric(df['PromotionDiscount'], errors='coerce').fillna(0)
        
        df['total_sales'] = df['QuantityOrdered'] * df['ItemPrice']
        df['net_sale'] = df['total_sales'] - df['PromotionDiscount']
        
        df.drop_duplicates(subset='OrderId', keep='first', inplace=True)
        df = df[df['net_sale'] > 0]
        
        print("Data transformed successfully.")
        return df
    except Exception as e:
        print(f"Error during data transformation: {e}")
        raise

def load_data(df):
    try:
        conn = sqlite3.connect('/content/sales_data.db')
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sales_data (
                OrderId TEXT PRIMARY KEY,
                OrderItemId TEXT,
                QuantityOrdered INTEGER,
                ItemPrice REAL,
                PromotionDiscount REAL,
                total_sales REAL,
                net_sale REAL,
                region TEXT
            )
        ''')
        
        df.to_sql('sales_data', conn, if_exists='replace', index=False)
        
        conn.commit()
        conn.close()
        
        print("Data loaded successfully into SQLite database.")
    except Exception as e:
        print(f"Error during data loading: {e}")
        raise

def validate_data():
    try:
        conn = sqlite3.connect('sales_data.db')
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM sales_data')
        total_records = cursor.fetchone()[0]
        print(f"\nTotal Records: {total_records}")
        
        cursor.execute('SELECT region, SUM(total_sales) FROM sales_data GROUP BY region')
        sales_by_region = cursor.fetchall()
        print("\nTotal Sales by Region:")
        for region, sales in sales_by_region:
            print(f"Region {region}: {sales}")
        
        cursor.execute('SELECT AVG(net_sale) FROM sales_data')
        avg_sales = cursor.fetchone()[0]
        print(f"\nAverage Sales per Transaction: {avg_sales}")
        
        cursor.execute('''
            SELECT OrderId, COUNT(*)
            FROM sales_data
            GROUP BY OrderId
            HAVING COUNT(*) > 1
        ''')
        duplicates = cursor.fetchall()
        if duplicates:
            print("\nDuplicate OrderId Found:")
            for order in duplicates:
                print(order)
        else:
            print("\nNo duplicate OrderId found.")
        
        conn.close()
    except Exception as e:
        print(f"Error during data validation: {e}")
        raise

if __name__ == '__main__':
    print("Starting ETL Pipeline...")
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)
    validate_data()
    print("\nETL Pipeline completed successfully.")
