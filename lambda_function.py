import pandas as pd
import snowflake.connector
from snowflake.connector import errors
from dotenv import load_dotenv
import os
import json

def lambda_handler(event, context):
    # Load environment variables from .env file
    try:
        load_dotenv()

        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USERname'),
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
        cur.execute('select * from FER_TEST.CONTACT.userst LIMIT 5')
        data=cur.fetchall()
    
        df= pd.DataFrame(data, columns=[x[0] for x in cur.description])
        print(df)

        cur.close()
        conn.close()
        return {
            'statusCode': 200,
            'body': df.to_json(orient='records')  # Convert DataFrame to JSON
        }

    except snowflake.connector.errors.DatabaseError as e:
        return {
            'statusCode': 500,
            'body': f"Database error: {e}"
        }
    except snowflake.connector.errors.OperationalError as e:
        return {
            'statusCode': 500,
            'body': f"Operational error: {e}"
        }
    except snowflake.connector.errors.ForbiddenError as e:
        return {
            'statusCode': 403,
            'body': f"Forbidden error: {e}"
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"An unexpected error occurred: {e}"
        }