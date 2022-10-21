import pandas as pd
from pandasql import sqldf as ps
import logging
import json
from pandas.io.json import json_normalize

logging.basicConfig(filename='log_file.log', level=logging.INFO)

total_file_path = './output/Total_value.csv'
Unique_file_path = './output/Unique_products.csv'
Classic_cars_price_month_file_path = './output/Classic_cars_price_month.csv'
Classic_cars_price_year_file_path = './output/Classic_cars_price_year.csv'
Discount_file_path = './output/Discount.csv'
 

def read_json(path: str):
    """ This function reads the JSON Files from path passed in function, It reads JSON files as objects then the
    objects are normalized and  converted to pandas dataframe
    :param path: String which points to the location of file
    :type path: String
    :return: Dataframe after being converted from JSON
    :rtype: Spark Dataframe"""

    with open(path) as f:
        read_obj = json.load(f)
    df_norm = pd.json_normalize(read_obj, record_path='attributes', meta=['ORDERNUMBER', 'PRODUCTCODE'])
    return df_norm


def check_sales_data(df_check: pd.DataFrame):
    """
    This function does the quality check for the data, in this function we drop duplicates, nulls and check the
     data type
    :param df_check: Dataframe which gets imported
    :type df_check: pandas dataframe
    :return: if dataframe passes all data checks
    :rtype: pandas dataframe
    """
    try:
        logging.info('Data Quality Check started')
        dt_chck = df_check.dtypes
        if dt_chck['QUANTITYORDERED'] != 'int64':
            raise Exception("Data Quality check Fail: datatype in QUANTITYORDERED")

        if dt_chck['PRICEEACH'] != 'float64':
            raise Exception("Data Quality check Fail: datatype in PRICEEACH")
        if dt_chck['SALES'] != 'float64':
            raise Exception("Data Quality check Fail: datatype in SALES")
        if dt_chck['ORDERDATE'] != 'object':
            raise Exception("Data Quality check Fail: datatype in ORDERDATE")
        if dt_chck['STATUS'] != 'object':
            raise Exception("Data Quality check Fail: datatype in STATUS")
        if dt_chck['PRODUCTLINE'] != 'object':
            raise Exception("Data Quality check Fail: datatype in PRODUCTLINE")
        if dt_chck['MSRP'] != 'int64':
            raise Exception("Data Quality check Fail: datatype in MSRP")
        if dt_chck['ORDERNUMBER'] != 'object':
            raise Exception("Data Quality check Fail: datatype in ORDERNUMBER")
        if dt_chck['PRODUCTCODE'] != 'object':
            raise Exception("Data Quality check Fail: datatype in PRODUCTCODE")

        df_check = df_check.dropna()
        df_check = df_check.drop_duplicates()
        logging.info('Data Quality Check Finished')
        return df_check
    except Exception as e:
        logging.info('Error in DataQuality Check: check_sales_data')
        logging.error(e)


def merger_dt(df_first: pd.DataFrame, df_second: pd.DataFrame, df_third: pd.DataFrame):
    """
    The Function merge all dataframes and doing the data quality check
    :param df_first: First input DataFrame
    :type df_first:  DataFrame
    :param df_second: Second input DataFrame
    :type df_second: DataFrame
    :param df_third: Third input DataFrame
    :type df_third: DataFrame
    :return: Union of all DataFrames into one dataframe
    :rtype: DataFrame
    """
    try:
        logging.info('Data Merger started')
        df_lst = [df_first, df_second, df_third]
        df_mer = pd.concat(df_lst)
        logging.info('Datasets successfully merged')
        return df_mer

    except Exception as e:
        logging.info('Error in Merging dataframes: merger_dt')
        logging.error(e)


def extract_year_month(df: pd.DataFrame):
    """
    This function extracts the Date,Year and Month from ORDERDATE column whis is of object type
    :param df: DataFrame
    :type df: DataFrame
    :return: Dataframe with 3 new columns(Date,Year,Month)
    :rtype: DataFrame
    """
    try:
        logging.info('Started extracting date,month and year')
        df['Year'] = (pd.DatetimeIndex(df['ORDERDATE'])).year
        df['Month'] = (pd.DatetimeIndex(df['ORDERDATE'])).month
        df['Date'] = (pd.DatetimeIndex(df['ORDERDATE'])).date
        return df
    except Exception as e:
        logging.info('Error in Extracting Month,date,year : extract_year_month')
        logging.error(e)


def save_parquet(df: pd.DataFrame):
    """
    This function save the data into parquet format, into partitions based on Date column in data
    :param df: dataset
    :type df: pandas dataframe
    :return: (Saves dataframe into parquet files)
    :rtype: parquet
    """
    try:
        df.to_parquet('sales_data', engine='auto', partition_cols=['Date'])
        logging.info('Data saved in parquet format Successfully')
    except Exception as e:
        logging.info('Error is Saving data in parquet: save_parquet')
        logging.error(e)


def total_value(df_tv: pd.DataFrame , file_path: str):
    """
      This function calculates the Total value where Status is cancelled or On hold
    :param df_tv:
    :type df_tv: pandas Dataframe
    :param file_path: Path for the file Csv
    :type file_path: String
    :return: Dataframe with Three columns [Status,Year,Total](filename: Total_value.csv)
    :rtype: DataFrame
    """

    try:
        q1 = """SELECT STATUS,YEAR,ROUND(sum(SALES),2) as Total FROM df_tv where \
        STATUS ='Cancelled' OR STATUS ='On Hold' group by STATUS,YEAR order by YEAR"""
        ps(q1, locals()).to_csv(file_path)
        logging.info('Total_value csv saved successfully')
    except Exception as e:
        logging.info('Error in calculating total value: total_value')
        logging.error(e)


def uniq_products(df_uniq_pro: pd.DataFrame , file_path: str):
    """
    This function Counts the unique product in each Productline
    :param: Takes input as pandas dataFrame
    :type: pandas dataframe
    :param file_path: Path for the file Csv
    :type file_path: String
    :return: Returns CSV with two columns [ Productline, Unique_products]. Unique_products gives the count of unique
    products in Productline.
    :rtype: CSV
    """
    try:
        q2 = """select PRODUCTLINE, count(DISTINCT  PRODUCTCODE) as Unique_products from \
        df_uniq_pro group by PRODUCTLINE"""
        ps(q2, locals()).to_csv( file_path)
        logging.info('Unique Products csv saved successfully')
    except Exception as e:
        logging.info('Error in calculating Unique products: uniq_products')
        logging.error(e)


def classic_cars(df_classic_cars: pd.DataFrame , file_path_month: str ,file_path_year: str):
    """
     This function gives the analysis of Sales of classic cars. The function calculates Number of cars sold
     per month and total amount(outputs result into Classic_cars_price_month.csv), also it calculates total cars
     sold each year and avg price of each car(outputs result into Classic_cars_price_year.csv)
    :param: Input takes as DataFrame, with columns - PRODUCTLINE, MONTH, YEAR, Sales
    :type:  DataFrame
    :param file_path: Path for the file Csv
    :type file_path: String
    :return: Returns two CSV files Classic_cars_price_month.csv,Classic_cars_price_year.csv
    :rtype: CSV
    """
    try:
        q3 = """select PRODUCTLINE,MONTH,YEAR,COUNT(*) as Count_per_month,ROUND(sum(SALES),2)  as Total_sale \
        from df_classic_cars where PRODUCTLINE = 'Classic Cars' group by PRODUCTLINE,MONTH,YEAR order by Month,Year"""
        ps(q3, locals()).to_csv(file_path_month)
        logging.info('calssic_cars_price_month csv saved successfully')

        q4 = """select PRODUCTLINE,YEAR,COUNT(*) as Count_per_year,ROUND(sum(SALES),2)  as Total_sale, \
        ROUND(avg(SALES),2) as avg_price_per_vehical from df_classic_cars where PRODUCTLINE = 'Classic Cars' \
        group by YEAR order by Year """
        ps(q4, locals()).to_csv(file_path_year)
        logging.info('avg_price_per_vehical csv saved successfully')

    except Exception as e:
        logging.info('Error at: classic_cars')
        logging.error(e)


def discount_msrp(df_discount: pd.DataFrame , file_path: str):
    """
     This function filters the dataframe where column PRODUCTLINE is in 'Vintage Cars','Classic Cars','Motorcycles',
     'Trucks and Buses' and calculates the discount According to MSRP value, this function also uses function
     'function_p' to generate 'Discounted_price' column.
    :param:  Takes Input as DataFrame
    :type: pandas DataFrame
    :param file_path: Path for the file Csv
    :type file_path: String
    :return: Returns CSV name: Discount.csv
    :rtype: CSV
    """
    try:
        q4 = """select * from df_discount where PRODUCTLINE in 
        ('Vintage Cars','Classic Cars','Motorcycles','Trucks and Buses') """
        df_discount = ps(q4, locals())
        df_discount['Discounted_price'] = df_discount['SALES']*((100 - (df_discount['MSRP'].apply(discount_p)))/100)
        df_discount.to_csv(file_path)
        logging.info('Discount csv saved successfully')
    except Exception as e:
        logging.info('Error in calculating Discount: discount_msrp')
        logging.error(e)


def discount_p(num: float):
    """
    This function takes float as input and predicts the range and returns the value accordingly
    :param num: number
    :type num: float
    :return: number
    :rtype: float
    """
    try:
        if num > 0 and num <= 30:
            return 0
        elif num > 30 and num <= 60:
            return 2.5
        elif num > 60 and num <= 80:
            return 4
        elif num > 80 and num <= 100:
            return 6
        elif num > 100:
            return 10
    except Exception as e:
        logging.info('Error in discount_p ')
        logging.error(e)


if __name__ == '__main__':
    data_SalesData_2003 = read_json('./source/SalesData_2003.json')
    data_SalesData_2004 = read_json('./source/SalesData_2004.json')
    data_SalesData_2005 = read_json('./source/SalesData_2005.json')
    check_sales_data(data_SalesData_2005)
    check_sales_data(data_SalesData_2004)
    check_sales_data(data_SalesData_2003)

    df_merged = merger_dt(data_SalesData_2003, data_SalesData_2004, data_SalesData_2005)
    discount_msrp(df_merged ,Discount_file_path)

    uniq_products(df_merged ,Unique_file_path)
    df_with_Date_Time = extract_year_month(df_merged)

    save_parquet(df_with_Date_Time)
    total_value(df_with_Date_Time , total_file_path)

    classic_cars(df_with_Date_Time ,file_path_month=Classic_cars_price_month_file_path ,file_path_year=Classic_cars_price_year_file_path)
