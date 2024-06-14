import pyspark.sql.functions as f

class FrameUtilsException(Exception):
    pass

class FrameUtils:
    @staticmethod
    def read_file(spark, princpal_path, file_config):
        """
        Reads a file using Apache Spark.

        Parameters:
            spark (pyspark.sql.SparkSession): The SparkSession object.
            principal_path (str): The principal path where the file is located.
            file_config (dict): A dictionary containing configuration options for reading the file.
                It should contain the following keys:
                - 'format_type' (str): The format of the file to be read (e.g., 'csv', 'parquet').
                - 'file_path' (str): The relative path of the file from the principal path.

        Returns:
            pyspark.sql.DataFrame: A DataFrame containing the data read from the file.

        Raises:
            Exception: If an error occurs during file reading.

        Example:
            # Read a CSV file
            spark_session = SparkSession.builder.appName("example").getOrCreate()
            config = {'format_type': 'csv', 'file_path': 'data.csv'}
            data = read_file(spark_session, '/path/to/principal', config)
        """
        try:
            FormatType = file_config.get("format_type")
            file_path = ""

            if "file_path" in file_config:
                file_name = file_config.get("file_path")
                file_path = f"{princpal_path}/{file_name}"
            if "header" in file_config:
                withHeader = file_config.get("header")

            print(f"Reading file from {file_path}")
            if FormatType in ["csv"]:
                withHeader = "true"
                separator_char = ","

                data_frame = spark.read \
                    .options(header=withHeader,
                            separator=separator_char) \
                    .format(FormatType) \
                    .load(file_path)
            elif FormatType in ["text"]:
                data_frame = spark.read \
                    .text(file_path,wholetext=True)
            else:
                data_frame = spark.read \
                    .format(FormatType) \
                    .load(file_path)

            data_frame.printSchema()
            return data_frame
        except Exception as exc:
            print(exc)
            return None

    @staticmethod
    def write_file(data_frame, princpal_path, file_config):
        """
        Writes a DataFrame to a file using Apache Spark.

        Parameters:
            data_frame (pyspark.sql.DataFrame): The DataFrame to be written to the file.
            principal_path (str): The principal path where the file will be saved.
            file_config (dict): A dictionary containing configuration options for writing the file.
                It should contain the following optional keys:
                - 'file_format' (str): The format in which the file will be saved (default: 'parquet').
                - 'save_mode' (str): The mode for saving the file (default: 'overwrite').
                - 'repartition' (int): The number of partitions to use when saving the file (default: 1).
                - 'file_path' (str): The relative path of the file to be saved from the principal path.
                - 'separator' (str): The separator character for CSV files (default: ',').
                - 'withHeader' (bool): Whether to include a header in CSV files (default: True).

        Returns:
            bool: True if the file was successfully written, False otherwise.

        Raises:
            Exception: If an error occurs during file writing.

        Example:
            # Write a DataFrame to a Parquet file
            spark_session = SparkSession.builder.appName("example").getOrCreate()
            config = {'file_format': 'parquet', 'file_path': 'output.parquet'}
            success = write_file(dataframe, '/path/to/principal', config)
            if success:
                print("File successfully written!")
        """
        file_format = "csv"
        save_mode = "overwrite"
        repartition = 1
        header = "true"


        if "file_format" in file_config:
            file_format = file_config.get("file_format")
        if "save_mode" in file_config:
            save_mode = file_config.get("save_mode")
        if "repartition" in file_config:
            repartition = file_config.get("repartition")
        if "header" in file_config:
            header = file_config.get("header")

        file_path = file_config.get("file_path")
        file_path = f'{princpal_path}/{file_path}'
        try:
            if file_format in ["csv"]:
                data_frame.repartition(repartition).write.format(
                    file_format).options(header=header,
                            separator=",") \
                            .mode(save_mode).save(file_path)
            else:
                data_frame.repartition(repartition).write.format(
                    file_format).mode(save_mode).save(file_path)
            return True
        except Exception as exc:
            print(exc)
            raise exc

    @staticmethod
    def cast_data_type(frame, transf_config):
        for column in transf_config:
            col_cast = transf_config.get(column)
            frame = frame.withColumn(column,
                                                f.col(column).cast(col_cast))
        return frame
    
    @staticmethod
    def select_cols(frame, trans_config):
        frame = frame.select(*trans_config)
        return frame
    
    @staticmethod
    def drop_duplicates(frame, trans_config):
        frame = frame.drop_duplicates(trans_config)
        return frame

    @staticmethod
    def cast_to_dictionary(frame, trans_config):
        new_col_name = trans_config["new_column_name"]
        frame = frame.withColumn(
            new_col_name, f.create_map(trans_config["columns"])).select(f.to_json(new_col_name).alias(new_col_name))
        return frame
    
    @staticmethod
    def cast_dictionary_to_col(frame, trans_config):
        mapping = trans_config["mapping"]
        col_names = trans_config["col_names"]
        dict_col_name = trans_config["dict_col_name"]
        frame = frame.withColumn("json_data_tmp", f.from_json(f.col(dict_col_name), mapping))
        frame = frame.select(f.explode(f.col("json_data_tmp")).alias(*col_names))
        return frame
