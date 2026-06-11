#This DAG is a more refined and newer version of the first DAG and uses
#features such as Decorators that are available in the reetn version of Airflow

from airflow.decorators import dag, task #new task Decorator
from airflow.operators.bash import BashOperator
from datetime import datetime


@dag(start_date=datetime(2026, 1, 1), description="training models", tags=["test case"], schedule="@daily", catchup=False) #with gauarantees exitm as opposed to not using it. CONTEXT MANAGER
def my_dag2():

    @task #this is a decorator - now below we can define the function that PythonOperator used to execute
    def _training_model(accuracy): #this is a task with TaskID=_training_model
        return accuracy

    #we can remove training_model_A B and C from before
    #at the end we will substitute that with dynamic task mapping
    #basically if the number of tasks for this DAG changes from day to day, dynamic task mapping can handle it


    @task.branch #now we also have a decorater for the BranchPythonOperator
    def _choose_best_model(accuracies):  #you can see how the way accuracies are passed in after the inaccurate fucntino definition
        best_accuracy = max(accuracies)
        if (best_accuracy > 8):
            return 'accurate'
        return 'inaccurate'


    #the decorators are not available for the BashOperators, so we proceed as before
    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"

    )

    #same for inaccurate
    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"

    )
    #the ".expand" is dynamic task mapping being implemented
    _choose_best_model(_training_model.expand(accuracy=[5,10,6])) >> [accurate, inaccurate] #this is how you share data between tasks and also arrange them in the way you desire within the DAG


my_dag2()
