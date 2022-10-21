
# Pipeline's for Sales Data

    In this project we have created Pipelines for transforming sales data according to different business needs.

    code file: Sales_data_pipelines.py - Python Notebook(code written to perform the tasks)

    log_file.log - Log file which collects the logs of whole flow of Sales_data_pipelines


## Files within source folder

    SalesData_2003.json- Sales data of year 2003
    
    SalesData_2004.json- Sales data of year 2004
    
    SalesData_2005.json- Sales data of year 2005


### Files under sales_data folder

    Sales_data Folder- Folder consisting of data partitions on date
    in Parquet format


### Files under output folder
    
    Total_value.csv - the total value of cancelled orders, and total value of orders currently on hold 
    segmented by year. 
    created by function total_value()
    
    Unique_products.csv - unique products per product line
    created by function uniq_products()
    
    Classic_cars_price_month.csv - number of classic cars sold 
    per month each year.
    created by function classic_cars()
    
    Classic_cars_price_year.csv -  number of classic cars sold 
    each year and average price of classic car sold each year.
    created by function classic_cars()
    
    Discount.csv - File consisting of volume-based discount(%)
    created by function def discount_msrp()


### Files under test folder

    __init__.py
    test_python_cli.py
    test_python_cli.py.bak

### Files under doc folder

    instructions.pdf
    slides.pptx - Flow of code and results
    
    
    




