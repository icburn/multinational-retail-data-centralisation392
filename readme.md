Multinational Retail Data Centralisation

This project extracts data from multiple sources, including an s3 repository, a pdf and a json file.  It then cleans the data and uploads it to a new, local database.  This project has presented many challenges. These are the main ones:  Firstly, I was struggling to upload the extracted data to a new database.  This is because i was attempting to upload it to the source location, which was read only. Next i encountered several internal server errors whilst trying to access the store info using an API.  This turned out to be because there were 450 stores and the code was expecting more.  Finally, cleaning the data.  I had no idea where to begin with this and was confused by what code to write.  Eventually i created the new tables first, skimmed through them looking for abnormalities and then wrote the code to remove or correct them.  

Installation instructions - Installation is not required. 
Usage instructions - Download all files to the same folder and then run main.py.  Ensure a database called sales_data is setup in the postgres server on the local machine.
File structure of the project - The file is in 4 parts.  The main.py file is the file that controls everything.  Data_extraction.py is responsible for all data extraction functions.  database_utils.py is for interacting wiht the databases and files.  data_cleaning.py is responsible for all the data cleaning functions, across each data type.  

License information - Licencing is not required for use. 