from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
from functions import _download_datasets, _download_data_pivot, _clean_file, _generation_file, _check_results

docs = """
### Purpose
  This test consists in developing an ETL pipeline to extract internal pivot caches from consolidated reports made available by Brazilian government's regulatory agency for oil/fuels, ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis).
    
#### Inputs
    This DAG downloads 3 files:
    -> Sales, by distributors, of petroleum fuel derivatives by UF and product.
    -> Sales, by distributors, of diesel oil by type and state.
    -> Pivot caches from consolidated reports made available by the Brazilian government's regulatory agency for oil/fuels

  These files will be used for the Analysis. 
    
#### Outputs
    This pipeline produces a file containing 5 sheets:
    - dags/dados/data_extracted.xlsx
        -> Sheet 01 - Derived Data.
        -> Sheet 02 - Diesel Data
        -> Sheet 03 - Data. Consolidated Derivatives and Diesel.
        -> Sheet 04 - Checks if the pivot data Totals are equal with the data extracted from Derivatives.
        -> Sheet 05 - Checks if the pivot data Totals are the same with the data extracted from Diesel.
    
#### Questions
  For any questions, please contact 
  [anders.des@gmail.com](mailto:anders.des@gmail.com).
"""
  
# parametros
default_args = {
    'owner': 'raizen',
    'start_date': datetime(2022, 7, 1),
    'depends_on_past': False
}

with DAG(
    'raizen_test',
    schedule_interval=None, 
    catchup=False,
    tags=['test', 'etl'],
    default_args=default_args,
    doc_md=docs,
) as dag:
    
    start = DummyOperator(
        task_id="start"
    ) 

    extract_datasets = PythonOperator(
        task_id="extract_datasets", 
        dag=dag,
        python_callable=_download_datasets
    )
 
    extract_pivot = PythonOperator(
        task_id="extract_pivot", 
        dag=dag,
        python_callable=_download_data_pivot
    )
     
    clean_files = PythonOperator(
        task_id="clean_files", 
        dag=dag,
        python_callable=_clean_file
    )
    
    generation_file_final = PythonOperator(
        task_id="generation_file_final", 
        dag=dag,
        python_callable=_generation_file
    )

    check_results = PythonOperator(
        task_id="check_results", 
        dag=dag,
        python_callable=_check_results
    )
    
    end = DummyOperator(
        task_id="end"
    ) 
                
    start >> [extract_datasets, extract_pivot] >> clean_files >> generation_file_final >> check_results >> end
    