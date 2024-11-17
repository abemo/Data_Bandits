import psycopg2
import time
import statistics
from utils import display_results

import config

def run_queries_and_analyze():
    print("===== PostgreSQL Query Performance Analysis =====")
    # Connection details
    db_config = {
        'dbname': config.POSTGRES_DBNAME,
        'user': config.POSTGRES_USERNAME,
        'password': config.POSTGRES_PASSWORD,
        'host': config.POSTGRES_HOST,
        'port': config.POSTGRES_PORT
    }
    
    # Define three queries to execute
    queries = [
        """
        SELECT
        a.account_id,
        COUNT(DISTINCT t.trans_id) AS transaction_count,
        SUM(t.amount) AS total_amount,
        AVG(t.amount) AS avg_transaction_amount,
        (
            SELECT MAX(t2.amount)
            FROM trans t2
            WHERE t2.account_id = a.account_id
        ) AS max_transaction_amount
        FROM
            account a
        LEFT JOIN
            trans t ON a.account_id = t.account_id
        LEFT JOIN
            disp d ON a.account_id = d.account_id
        LEFT JOIN
            client c ON d.client_id = c.client_id
        WHERE
            t.date BETWEEN CURRENT_DATE - INTERVAL '1 year' AND CURRENT_DATE
        GROUP BY
            a.account_id
        HAVING
            COUNT(DISTINCT t.trans_id) > 100
        ORDER BY
            total_amount DESC
        LIMIT 100;
        """,  # Query 1
        """
        WITH ranked_transactions AS (
        SELECT
        t.account_id,
        t.trans_id AS transaction_id,
        t.amount,
        t.date AS transaction_date,
        ROW_NUMBER() OVER (PARTITION BY t.account_id ORDER BY t.amount DESC) AS amount_rank,
        AVG(t.amount) OVER (
            PARTITION BY t.account_id
            ORDER BY t.date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS moving_avg
        FROM
            trans t
        JOIN
            account a ON t.account_id = a.account_id
        JOIN
            disp d ON a.account_id = d.account_id
        JOIN
            client c ON d.client_id = c.client_id
        WHERE
            d.type = 'OWNER'
            AND t.date >= CURRENT_DATE - INTERVAL '6 months'
        )
        SELECT
            rt.account_id,
            COUNT(*) AS high_value_transactions,
            AVG(rt.amount) AS avg_high_value_amount,
            MAX(rt.moving_avg) AS max_moving_avg
        FROM
            ranked_transactions rt
        WHERE
            rt.amount_rank <= 10
            AND rt.amount > rt.moving_avg * 1.5
        GROUP BY
            rt.account_id
        HAVING
            COUNT(*) > 5
        ORDER BY
            avg_high_value_amount DESC;
        """, # Query 2
        """
        WITH
        daily_deposits AS (
            SELECT
            c.client_id,
            t.date AS date,  -- Use the date field from the trans table
            SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS daily_deposit,
            SUM(CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END) AS daily_withdrawal
            FROM
            client c
            LEFT JOIN disp d ON c.client_id = d.client_id
            LEFT JOIN account a ON d.account_id = a.account_id
            LEFT JOIN trans t ON a.account_id = t.account_id
            WHERE
            t.date >= CURRENT_DATE - INTERVAL '1 year'  -- Use date here instead of timestamp
            GROUP BY
            c.client_id, t.date  -- Group by the date field from the trans table
        ),
        daily_loans AS (
            SELECT
            c.client_id,
            l.date AS date,  -- Use the date field from the loan table (if exists)
            SUM(l.amount) AS total_loan_value
            FROM
            client c
            LEFT JOIN disp d ON c.client_id = d.client_id
            LEFT JOIN account a ON d.account_id = a.account_id
            LEFT JOIN loan l ON a.account_id = l.account_id
            WHERE
            l.date >= CURRENT_DATE - INTERVAL '1 year'  -- Use date here instead of timestamp
            GROUP BY
            c.client_id, l.date  -- Group by the date field from the loan table
        ),
        combined_data AS (
            SELECT
            dd.client_id,
            dd.date,
            dd.daily_deposit,
            dd.daily_withdrawal,
            COALESCE(dl.total_loan_value, 0) AS total_loan_value
            FROM
            daily_deposits dd
            LEFT JOIN
            daily_loans dl ON dd.client_id = dl.client_id AND dd.date = dl.date
        ),
        rolling_sums AS (
            SELECT
            client_id,
            date,
            daily_deposit,
            daily_withdrawal,
            total_loan_value,
            SUM(daily_deposit) OVER (PARTITION BY client_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_deposit_30_days,
            SUM(daily_withdrawal) OVER (PARTITION BY client_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_withdrawal_30_days,
            SUM(total_loan_value) OVER (PARTITION BY client_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_loan_value_30_days
            FROM
            combined_data
        )
        SELECT
        client_id,
        date,
        daily_deposit,
        daily_withdrawal,
        total_loan_value,
        rolling_deposit_30_days,
        rolling_withdrawal_30_days,
        rolling_loan_value_30_days
        FROM
        rolling_sums
        ORDER BY
        client_id, date DESC
        LIMIT 1000;
        """  # Query 3
        
    ]
    
    # Number of executions per query
    iterations = config.NUMBER_ITERATIONS
    results = []

    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        print("Connected to the database successfully.")
        
        for query in queries:
            times = []  # Store execution times for the current query
            
            for _ in range(iterations):
                start_time = time.time()
                cursor.execute(query)
                cursor.fetchall()  # Ensure the query completes execution
                end_time = time.time()
                times.append(end_time - start_time)
            
            # Calculate metrics
            avg_time = sum(times) / iterations
            std_dev = statistics.stdev(times)
            max_time = max(times)
            
            # Append results
            results.append((query, avg_time, std_dev, max_time))
    
    except Exception as e:
        print(f"Error occurred: {e}")
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Database connection closed.")

    # Display results in a table
    display_results(results)


if __name__ == "__main__":
    run_queries_and_analyze()
