import pandas as pd
import snowflake.connector
from snowflake.connector import errors
from dotenv import load_dotenv
import os

load_dotenv()

try:

    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE'),
    )

    # Create a cursor object
    cur = conn.cursor()

    print("okok")

    date = '2022-11-01'
    date_range1='2022-01-01'
    date_range2='2022-12-01'

    #cur.execute('select * from demokit_finance.staging.balance_sheet LIMIT 20')

    """
    cur.execute('''SELECT 
                        revenue, 
                        net_profit, 
                        (net_profit / revenue) * 100 AS net_profit_margin
                    FROM 
                        demokit_finance.staging.balance_sheet''')
    """

    """
    cur.execute('''
                    SELECT revenue
                    FROM demokit_finance.staging.BALANCE_SHEET
                    WHERE year(date) = year(%s::TIMESTAMP) AND month(date) = month(%s::TIMESTAMP);
                ''', (date, date))
    """

    cur.execute('''
                    select revenue from demokit_finance.staging.BALANCE_SHEET
                    where date between %s and %s;
                ''', (date_range1, date_range2))

    data=cur.fetchall()
    
    print(data)

    df= pd.DataFrame(data, columns=[x[0] for x in cur.description])
    print(df)
    
    print("list")
    print(df["REVENUE"].tolist())
    #print(df["NET_PROFIT"][0])

    cur.close()
    conn.close()

    # Close the connection

except snowflake.connector.errors.DatabaseError as e:
    print(f"Database error: {e}")
except snowflake.connector.errors.OperationalError as e:
    print(f"Operational error: {e}")
except snowflake.connector.errors.ForbiddenError as e:
    print(f"Forbidden error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
