from common.frame_utils import FrameUtils

class TransformServiceException(Exception):
    pass

class TransformService:
    @staticmethod
    def transformations(frame, config_transformations):

        if "select_cols" in config_transformations:
            frame = FrameUtils.select_cols(frame, config_transformations.get("select_cols"))
        
        if "cast_data_type" in config_transformations:
            frame = FrameUtils.cast_data_type(frame, config_transformations.get("cast_data_type"))

        if "drop_duplicates" in config_transformations:
            frame = FrameUtils.drop_duplicates(frame, config_transformations.get("drop_duplicates"))

        if "cast_to_dictionary" in config_transformations:
            frame = FrameUtils.cast_to_dictionary(frame, config_transformations.get("cast_to_dictionary"))
        
        if "cast_dictionary_to_col" in config_transformations:
            frame = FrameUtils.cast_dictionary_to_col(frame, config_transformations.get("cast_dictionary_to_col"))

        return frame

    @staticmethod
    def executeQuery(spark, query):
        frame = spark.sql(query)
        return frame