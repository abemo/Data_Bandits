import psycopg2
import time
import statistics
from utils import display_results

import config

def run_queries_and_analyze():
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
            (SELECT MAX(amount)
            FROM trans
            WHERE account_id = a.account_id) AS max_transaction_amount
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
        SELECT
        c.client_id,
        SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS total_deposits,
        SUM(CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END) AS total_withdrawals,
        COUNT(DISTINCT l.loan_id) AS total_loans,
        SUM(l.amount) AS total_loan_value,
        COUNT(DISTINCT t.trans_id) AS total_transactions
        FROM
            client c
        LEFT JOIN
            disp d ON c.client_id = d.client_id
        LEFT JOIN
            account a ON d.account_id = a.account_id
        LEFT JOIN
            trans t ON a.account_id = t.account_id
        LEFT JOIN
            loan l ON a.account_id = l.account_id
        WHERE
            t.date >= CURRENT_DATE - INTERVAL '1 year'
        GROUP BY
            c.client_id
        ORDER BY
            total_deposits DESC
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
