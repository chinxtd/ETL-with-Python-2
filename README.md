# ETL-with-Python-2
ETL using Python (Pandas, ElementTree and glob modules)
- Extract 'Used Car' Price's data from multiple format (json,csv,xml) into single dataframe
  - Use ElementTree module to parse the XML file and put it in dataframe (csv and json can use pandas to read directly)
  - Use for loop with 'glob' module to combine multiple dataframe in to single dataframe
- Transform the Price of 'Used Car' into 2 decimal numbers
- Load data into .csv
- Run ETL with logging 
