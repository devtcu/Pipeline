from airflow import DAG

from random import randint

"""
A PythonOperator is an Airflow task that runs a Python function.
It's the most common operator — whenever you want a task in your DAG to execute Python code, you use it.    

A BranchPythonOperator is like a PythonOperator but it decides which task to run next based on logic you define
like an if/else for your DAG.
"""
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator #we can use this to operate bash commands


from datetime import datetime

""" 
When a PythonOperator task finishes, its return value is automatically saved
  to Airflow's database (Postgres) tagged with the task ID. That's XCom push —
  it happens implicitly.
  
  xcom_pull is how you retrieve those saved values in a later task:
"""
def _choose_best_model(ti): #ti is tank instance object
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    #we must return the task id of the next task we want to exectue with Branch Python Operator
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'

def _training_model():
    #lets return fake garbage for test
    return randint(1,10)


    

#creating instance of a DAG class
# parameters of DAG (Uniqe identifier, start date, schedule interval, catchup)
#the last paramter "catchup" should be set to false if you dont want the DAG to create an instance
#for everyday since the start date nand the current date
with DAG("my_dag", start_date=datetime(2026, 1, 1), description="training models", tags=["test case"], schedule="@daily", catchup=False): #with gauarantees exitm as opposed to not using it. CONTEXT MANAGER
    
    #lets create a task!
    training_model_A = PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model #this is the python function that we want to pull from the task
    )
    
    #since we have three tasks, we can do two more
    
    training_model_B = PythonOperator(
        task_id="training_model_B",
        python_callable=_training_model #this is the python function that we want to pull from the task
    )
    
    training_model_C = PythonOperator(
        task_id="training_model_C",
        python_callable=_training_model #this is the python function that we want to pull from the task
    )
    
    #now we want to chooose the best accuracy, and if that is above a threshold
    #we want to execute some task over another. this requires a differnet type of task
    
    choose_best_model = BranchPythonOperator(
        task_id="chose_best_model",
        python_callable=_choose_best_model
    )
    
    #lets create two tasks
    
    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
        
    )
    
    #same for inaccurate
    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
        
    )

    #to define the way in which tasks or going to be executed
    # >> is for downstream tasks
    # << is for upstream tasks
    #we want training_model tasks first
    
    [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]

