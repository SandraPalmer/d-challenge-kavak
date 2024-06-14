from pyspark.sql import SparkSession 
from common.common_utils import CommonUtils
from common.frame_utils import FrameUtils
from common.transform_service import TransformService

PATH_INPUT_DATA = "ejercicio-2/files"

if __name__ == "__main__":
    json_path_product_type = "ejercicio-2/data-config/product_type.json"
    json_path_articles_final = "ejercicio-2/data-config/articles_final.json"
    
    spark = SparkSession.builder.appName("d-challenge-kavak").getOrCreate()
    try:
        #Read articles file and cast to map and save in a csv file  -->files/product_type
        config_file = CommonUtils.load_json_file(json_path_product_type)
        frame = FrameUtils.read_file(spark, PATH_INPUT_DATA,config_file["input_config"])
        frame = TransformService.transformations(frame, config_file["transformations"])
        FrameUtils.write_file(frame, PATH_INPUT_DATA, config_file["output_config"]  )

        #Read product_type file, cast map type to column type and join with articles, save result file in -->files/final_file
        config_file = CommonUtils.load_json_file(json_path_articles_final)
        list_datasets = config_file["datasets"]
        query = config_file["query"]

        for dataset in list_datasets:
            dataset_name = dataset.get("dataset_name")
            frame = FrameUtils.read_file(spark, PATH_INPUT_DATA,dataset["input_config"])
            frame = TransformService.transformations(frame, dataset["transformations"])
            frame.createOrReplaceTempView(dataset_name)
        
        final_frame = TransformService.executeQuery(spark, query)

        FrameUtils.write_file(final_frame, PATH_INPUT_DATA, config_file["output_config"]  )


    except (Exception) as error:
        print(error)
    
    spark.stop()
