from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import DQC

# The Operarator ensure the DQC and do a count check of table

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)    
        for table in self.tables: 
            records = redshift.get_records(DQC.count_check.format(table))  
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error("{} returned no results".format(table))
                raise ValueError("Data quality check failed. {} returned no results".format(table))
            num_records = records[0][0]
            if num_records == 0:
                self.log.error("No records present in destination table {}".format(table))
                raise ValueError("No records present in destination {}".format(table))
            else :
                self.log.info("Data quality on table {} check passed with {} records".format(table, num_records))
                
        staging_counts = redshift.get_records( DQC.count_check.format(self.tables[0]) 
                                            + " WHERE page='NextSong'" )
        fact_counts = redshift.get_records( DQC.count_check.format(self.tables[2]))
        self.log.info("Staging Table and Fact Table Comparision:")
        self.log.info("Staging Table: {} - {}".format(self.tables[0], staging_counts[0][0]) )
        self.log.info("Fact Table: {} - {}".format(self.tables[2], fact_counts[0][0]) )
        if(staging_counts[0][0] == fact_counts[0][0]):
            self.log.info("Count between Staging Table and Fact Table matches.")
        else:
            self.log.info("Incorrect data in Fact Table.")
            raise ValueError("Incorrect data in Fact Table.")  
        self.log.info("data quality check completed")  
            
        
                          
            