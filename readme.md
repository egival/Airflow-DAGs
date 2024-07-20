# Airflow Learning Projects

This repository contains a collection of Airflow DAGs that I've built while following tutorials from Astronomer Academy and LinkedIn Learning. Some DAGs have been modified to explore concepts further/fixing small mistakes. All DAGs have been tested locally using the Airflow UI.

Corrections made:
* In the "Create Variable" video on Astronomer's Academy, the value of the variable is written as {"param": "[100, 150, 200]"}, which is incorrect and should be written as: {"param": [100, 150, 200]}.

Other observations:
* The LinkedIn Learnings instructor uses Xcoms to pass a dataframe containing 1350 records. However, this is not recommended in Airflow documentation as Xcoms are designed for small amounts of data. 
Example: Intermediate_dags\Branched_DAGs\Linkedin_courses\branching_with_variables.py

**Structure:**

The repository is organized by difficulty level:
* beginner: Basic Airflow concepts and tasks ("Hello World!" dag, Python operator, Bash Operator, validating and transforming data etc.)
* intermediate: More advanced concepts like defining dependencies, XComs exchange for data usage, creating and using variables, branching.

**Technologies:**

* Airflow
* Python

**Learning Resources:**

* Astronomer Academy (URL: https://academy.astronomer.io/path/airflow-101)
* LinkedIn Learning:
    https://www.linkedin.com/learning/learning-apache-airflow
    https://www.linkedin.com/learning/apache-airflow-essential-training


![alt text](screenshot_of_ml_dag.PNG)