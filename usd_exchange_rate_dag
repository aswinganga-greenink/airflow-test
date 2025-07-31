from __future__ import annotations

import json
import pendulum
import pandas as pd

from airflow.decorators import dag, task

# Define the DAG
@dag(
    dag_id="process_usd_exchange_rates",
    description="A simple ETL DAG to fetch USD exchange rates and save them.",
    start_date=pendulum.datetime(2025, 7, 31, tz="UTC"),
    schedule="0 0 * * *",  # Run once a day at midnight UTC
    catchup=False,
    tags=["example", "etl", "api"],
)
def process_usd_exchange_rates_dag():
    """
    ### Process USD Exchange Rates
    This DAG fetches the latest USD exchange rates from a public API,
    selects a few key currencies, and saves the result to a CSV file.
    """

    @task
    def get_usd_rates() -> dict:
        """
        Fetches the latest USD exchange rates from the ExchangeRate-API.
        The TaskFlow API will automatically push the returned dictionary to XComs.
        """
        import requests

        # Using a free, no-key-required API endpoint
        response = requests.get("https://open.er-api.com/v6/latest/USD")
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()

    @task
    def transform_rates(rates_json: dict) -> dict:
        """
        Extracts only the rates for INR, EUR, and GBP.
        """
        # The rates are in a nested dictionary under the 'rates' key
        all_rates = rates_json['rates']
        
        selected_rates = {
            "currency": ["INR", "EUR", "GBP"],
            "rate": [
                all_rates.get("INR"),
                all_rates.get("EUR"),
                all_rates.get("GBP")
            ],
            "last_updated_utc": rates_json['time_last_update_utc']
        }
        
        print(f"Processed Rates: {selected_rates}")
        return selected_rates

    @task
    def save_rates_to_csv(processed_rates: dict):
        """
        Saves the processed exchange rates into a CSV file.
        The file will be saved in the Airflow include directory by default.
        """
        # Define the output file path. You might need to adjust this
        # based on your Airflow setup (e.g., use a mounted volume).
        output_path = "/opt/airflow/data/usd_rates.csv"
        
        # Create a pandas DataFrame from the dictionary
        df = pd.DataFrame(processed_rates)
        
        # Ensure the directory exists
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save the DataFrame to a CSV file
        df.to_csv(output_path, index=False)
        print(f"Successfully saved rates to {output_path}")


    # Set dependencies using the TaskFlow API
    raw_rates = get_usd_rates()
    transformed_data = transform_rates(raw_rates)
    save_rates_to_csv(transformed_data)

# Instantiate the DAG
process_usd_exchange_rates_dag()
