#  data context
import time
from uuid import uuid4 
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig


now =time.localtime()
class DQBGe():
    def __init__(
        self,
        job_args 
        


    ) -> None:
        self.job_args = job_args
        self.context = self._create_data_context()

    def _create_data_context(self):
        expectations_prefix = self.job_args.get('expectations_prefix')
        expectations_bucket = self.job_args.get('expectations_bucket')
        assets_bucket = self.job_args.get('assets_bucket')       

        data_context_config = DataContextConfig(
            config_version=3.0,
            plugins_directory=None,
            config_variables_file_path=None,
            # stores 
            stores={
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",
                        "bucket": expectations_bucket,
                        "prefix": expectations_prefix,
                    },
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",                        
                        "bucket": assets_bucket,
                        "prefix": 'validations/',
                    }
                },
                "checkpoint_store": {
                    "class_name": "CheckpointStore",            
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                    },
            },

            #     # store names
            expectations_store_name="expectations_store",
            validations_store_name="validations_store",
            evaluation_parameter_store_name="evaluation_parameter_store",
            checkpoint_store_name = "checkpoint_store",


            #     # data docs
            data_docs_sites={
                "s3_site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",
                        "bucket":  assets_bucket,
                        
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder",
                        "show_cta_footer": True,
                    },
                }
            },
        #     # # validations
        #     # validation_operators={},
        )

        context = BaseDataContext(project_config=data_context_config) 

        return context

    def create_data_source(self, data_source_name):
        #     #run time datasource
        print(data_source_name)
        datasource_config = {    
            "name": data_source_name,    
            "class_name": "Datasource",    
            "module_name": "great_expectations.datasource",    
            "execution_engine": {        
                "module_name": "great_expectations.execution_engine",        
                "class_name": "SparkDFExecutionEngine",  
                "force_reuse_spark_context": True  
                },    
                "data_connectors": {        
                    "spark_runtime_data_connector": {            
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["dynamo_id"],
                        },    
                    },
                }

        return datasource_config


    def create_check_point(self, checkpoint_name, expectations_name):
        
        check_point_config = {    
            "name": checkpoint_name,  
            "class_name": "Checkpoint", 
            "config_version": 1,     
            "runtime_configuration": {        
                "result_format": {            
                    "result_format": self.job_args.get('result_format'),        
                    "include_unexpected_rows": True    
                },         
            }, 
            "run_name_template": expectations_name,
            "batch_request": {},
            "expectation_suite_name": expectations_name,

            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],   
            
            
            "validations": [],
        }  
        return check_point_config

    def create_batch_request(self, batch_data, dynamo_db_id, data_asset_name, data_source_name):

        batch_request = RuntimeBatchRequest(    
            datasource_name=data_source_name,    
            data_connector_name="spark_runtime_data_connector",    
            data_asset_name= data_asset_name,  # This can be anything that identifies this data_asset for you    
            runtime_parameters={
                "batch_data": batch_data
                },  # Pass your DataFrame here.    
            batch_identifiers={                
                    "dynamo_id": dynamo_db_id
               },
        )
        return batch_request

    def __str__(self):
        return 'dqb ge instance' #will come up with a better name

   

if __name__ == '__main__':

    print('tests')
   