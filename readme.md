# Sparkify Cloud Data PipeLine

## Introduction
This project creates a pipeline using Airflow to extract, transform and loads 5 main informations into redshift DB from the song and event datasets(json Files) uploaded in s3 bucket and will do a DQC(Data Quality Check) after Loading of data:

### staging tables
- staging_events
- staging_songs

### Analytical tables
- songplays  - (Fact Table)
- users    }
- songs    } - (Dimension Table)
- artists  }
- time - (timestamps breakdown into comprehensible columns)

## DAG Design
For illustration purposes you can check out the graph that represents this pipeline's flow:
[Database Schema!](img/DAG.PNG "DAG structure")

### Briefly talking about this ELT process:

- Stages the raw data;
- then transform the raw data to the songplays fact table;
- and transform the raw data into the dimensions table too;
- finally, check if the fact/dimensions table has at least one row.


# Add Airflow Connections
Here, we'll use Airflow's UI to configure your AWS credentials and connection to Redshift.

1. To go to the Airflow UI:
2. Click on the Admin tab and select Connections.
3. Under Connections, select Create.
4. On the create connection page, enter the following values:

   Conn Id  : <Enter aws_credentials>
   Conn Type: <Enter Amazon Web Services>
   Login    : <Enter your Access key ID from the IAM User credentials>
   Password : E<nter your Secret access key from the IAM User >
    
Once you've entered these values, select Save and Add Another.
  
# On the next create connection page, enter the following values:

    Conn Id: <Enter redshift>
	Conn Type: <Enter Postgres>
	Host: <Enter the endpoint of your Redshift cluster>
	Schema: <Enter dev. This is the Redshift database you want to connect to>
	Login: <Enter awsuser>
	Password: <Enter the password>
	Port: <Enter 5439>
Once you've entered these values, select Save.
    
## Datasets
For this project, you'll be working with two datasets. Here are the s3 links for each:

Log data : <s3://udacity-dend/log_data>
Song data: <s3://udacity-dend/song_data>
  
## Author
**Aditya Dhanraj** - [Linkedin Profile](https://www.linkedin.com/in/aditya-dhanraj).