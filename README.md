# Airflow Dag : load local csv data into MSSQL database

![](https://github.com/chanchanngann/airflow_local_to_mssql/blob/master/images/02_visuals.png)



### Objective
__To automate the process of loading csv data from local path into MSSQL database using Airflow.__

### Flow
![](https://github.com/chanchanngann/airflow_local_to_mssql/blob/master/images/01_flow.png)

### Some good practices to follow

1. Avoid Top level Python Code
   you should not write any code outside the tasks. 
2. Avoid top level imports -> use local imports inside the execute() methods of the operators.
   Top-level imports might take surprisingly a lot of time and they can generate a lot of overhead.
3. Avoid Airflow Variables in Top level Python Code
   use the Airflow Variables inside the execute() methods of the operators.
4. Specify configuration details consistently
   Specify the configuration values in a single location (e.g., a shared YAML file), following the DRY (don’t repeat yourself) principle.
5. make your DAGs more linear 
   The DAG that has simple linear structure A -> B -> C will experience less delays in task scheduling than 
   DAG that has a deeply nested tree structure with exponentially growing number of depending tasks for example. 

## Conclusion

In this exercise I pulled data from local csv path, and inserted the data into mssql database with the help of airflow.

*References:*
- https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
- https://github.com/pymssql/pymssql/blob/master/docs/pymssql_examples.rst
- Book: Data Pipelines with Apache Airflow (p.260-265)
