import json
from json.decoder import JSONDecodeError



class CommonUtilsException(Exception):
    pass


class CommonUtils:
    @staticmethod
    def load_json_file(file_path):
        """
        Loads JSON data from a file.

        Parameters:
            file_path (str): The path to the JSON file.

        Returns:
            dict: A dictionary containing the JSON data.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            JSONDecodeError: If the JSON data cannot be decoded.
            CommonUtilsException: If an error occurs during the loading process.

        Example:
            # Load JSON data from a file
            data = load_json_file("data.json")
            print(data)
        """
        try:
            with open(file_path, encoding="utf-8") as json_file:
                return json.load(json_file)
        except (FileNotFoundError, JSONDecodeError) as error:
            print(f"The file {file_path} cannot be loaded")
            raise CommonUtilsException(error) from error

