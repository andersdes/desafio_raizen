from ast import If
import win32com.client as win32
import pandas as pd
import numpy as np
import os, re
from datetime import datetime 
win32c = win32.constants

def load_workbook():
    """
    Load the specified excel file for the start of the test.
        
    Returns
    -------
    Workbook : object
        Returns a Workbooks collection representing all open work tabs.
    """
    file = 'https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls'
    #file = os.path.dirname(os.path.abspath(__file__)) + '/vendas-combustiveis-m3.xls'
    
    # create excel object
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    # excel can be visible or not
    excel.Visible =  False
    # load workbook
    wb = excel.Workbooks.Open(file)  
    
    return wb

def load_pivot_table(workbook, range_1, range_2):
    """
    Load from the specified range in which pivot table it will work.

    Parameters
    ----------
    workbook : object
        Workbooks collection representing all open work tabs.
    range_1 : string
        Represents the beginning of the range that will work.
    range_2 : string
        Represents the end of the range that will work.
                
    Returns
    -------
    pvtTable : object
        Returns the pivot table to be worked on.
    """    
    ws = workbook.Worksheets(1)
    pvtTable = ws.Range(range_1, range_2).PivotTable
    return pvtTable

def clean_filter(pvtTable, filter):
    """
    Clear the pivot table filter

    Parameters
    ----------
    pvtTable : object
        Pivot table 
    filter : string
        Filter name to be reset.
    """
    pvtTable.PivotFields(filter).ClearAllFilters()
    
def close_workbook(workbook):  
    """
    Close the object.

    Parameters
    ----------
    workbook : object
        Workbooks collection representing all open work tabs.           
    """
    workbook.Close(True)

def convert_list_to_df(table_data, item_columns):
    """
    Convert pivot table list to dataframe

    Parameters
    ----------
    table_data : array
        Pivot table.
    item_columns : array
        Number of columns that should be applied to the reshape.

    Returns
    -------
    df : ndarray
        Returns the dataframe.

    """
    # Check list size
    list_length = len(table_data)
    
    # Adjust the number of columns to reshape
    column_df = len(item_columns) - 1
    # Check the number of rows based on the size of the list and columns
    row_df = int(list_length/column_df)   
   
    # reshape list into array
    arr2D = np.reshape(table_data, (row_df, column_df))
    # convert list in dataframe
    df = pd.DataFrame(arr2D)
    return df

def generator_dataframe(pvtTable, columns_df, column_pivot, filter_1, filter_2):
    """
    Generates the dataframe traversing the entire pivot table, applying the uf and product filters.

    Parameters
    ----------
    pvtTable : object
        Pivot table.
    columns_df : array
        List of columns applied to the dataframe.
    column_pivot : object
       List of columns applied to the table pivot.
    filter_1 : string
        Filter corresponding to the first of the pivot table.
    filter_2 : string
        Filter corresponding to the second of the pivot table.

    Returns
    -------
    df_merged : ndarray
        Returns the dataframe containing the result extracted from the pivot table.

    """
    # Contains all columns that must be extracted to the dataframe
    #item_columns = column_pivot + ['None']
    item_columns = column_pivot
    # Dataframe that will store the result.
    df_merged = pd.DataFrame()
    # Performs the first search by Federative Unit
    for item in range(1,pvtTable.PivotFields(filter_1).PivotItems().Count+1): 
        # Get result filter 1
        uf = pvtTable.PivotFields(filter_1).PivotItems(item)
        # Apply filter 1 to the pivot table
        pvtTable.PivotFields(filter_1).CurrentPage = uf.Caption
        # Performs the second search by Product
        for item2 in range(1,pvtTable.PivotFields(filter_2).PivotItems().Count+1): 
            table_data = ['UF', 'PRODUTO']
            # Get result filter 2
            prod = pvtTable.PivotFields(filter_2).PivotItems(item2)
            # Apply filter 2 to the pivot table
            pvtTable.PivotFields(filter_2).CurrentPage = prod.Caption
            # Cycles through the entire pivot table adding extra columns
            for i in pvtTable.TableRange1:
                if str(i) != 'None' and int(i.Column)==2 :
                    table_data.append(uf.Caption)
                    table_data.append(prod.Caption)
                table_data.append(str(i))
            
            # Convert a list in dataframe
            df = convert_list_to_df(table_data, item_columns)  
            df.columns = columns_df
            df = df.drop(index = 0)
            df = df.drop(index = 1)
            df = df.drop(index = 14)
            df_merged = pd.concat([df, df_merged], ignore_index=True, sort=False)
            #break
        #break
    return df_merged    

def clean_space_parentheses(str):
    """
    Checks the product name and removes the unit (m3) from the name.

    Parameters
    ----------
    str : String
        Product name.

    Returns
    -------
    str : String
        Returns the Product name.
    """  
      
    str = re.sub(r'\(\w+\)$',"",str)
    str = re.sub(r"\(\s+","(", str)
    str = re.sub(r"\s+\)",")", str)
    return str

def formated_year_month(year, month):
    """
    Format field year_month	date

    Parameters
    ----------
    year : String
        Year extraction from pivot table.
    month : String
        Month extraction from pivot table.
        
    Returns
    -------
    date : date
        Returns returns the date according to the past period.
    """  
    month_name = {
        'Janeiro': 1 ,
        'Fevereiro':2,
        'Março': 3,
        'Abril': 4,
        'Maio': 5,
        'Junho': 6,
        'Julho': 7,
        'Agosto': 8,
        'Setembro': 9,
        'Outubro': 10,
        'Novembro': 11,
        'Dezembro': 12        
    }
    date = datetime(int(year), month_name[month], 1)
    #return f'{str(year)}_{str(month_name[month])}'
    return date

def clean_dataframe(df):
    """
    Clean up the dataframe and name the columns.

    Parameters
    ----------
    df : ndarray
        Dataframe.

    Returns
    -------
    df : ndarray
        Dataframe.
    """  
    df = df.replace(['None'], 0.0)
    df['unit'] = df['produto'].apply(lambda x: x[len(x)-3:-1])
    df['volume'] = df['volume'].astype(float)
    df['product'] = df['produto'].apply(lambda x: clean_space_parentheses(x))
    df['year_month'] = df.apply(lambda x: formated_year_month(x['ano'], x['mes']), axis=1)
    df['created_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %X')
    
    df = df[['year_month', 'uf', 'product', 'unit', 'volume', 'created_at']]
    return df


def get_total_dataframe(df):
    """
    Performs the calculation of the total according to the past dataframe.

    Parameters
    ----------
    df : ndarray
        Data Frame.

    Returns
    -------
    df : ndarray
        Returns a dataframe with the consolidated total by year.
    """      
    df = df.replace(['None'], 0.0)
    df['year'] = df['ano'].apply(lambda x: x)
    df['volume'] = df['volume'].astype(float)
    df = df.query("mes == 'Total do Ano'")   

    # Using reset_index()
    df = df.groupby(['year'])['volume'].sum().reset_index()
    return df


def load_vars(name_pivot):
    # Filters Pivot table
    filters = ["UN. DA FEDERAÇÃO", "PRODUTO"]
    
    # Columns Pivot Table
    column_pivot = ['UF', 'PRODUTO','ANO', 'Dados', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', 
                '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016',
                '2017', '2018', '2019', '2020']
    # Columns Dataframe
    columns_df = ['uf', 'produto', 'mes', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', 
                '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016',
                '2017', '2018', '2019', '2020']
    
    # Define Variables
    vars = []
    ranges_pvt = []
    column_pvt = []
    column_df = []
    if name_pivot == 'pvt1':
        ranges_pvt = ["B49", "B65"]
        column_pvt = column_pivot
        column_df = columns_df
    else:
        ranges_pvt = ["B129", "B145"]
        column_pvt = column_pivot[0:4] + column_pivot[17:]
        column_df = columns_df[0:3] + columns_df[16:]
        
    # Add variables in vars        
    vars.append(filters)
    vars.append(ranges_pvt)
    vars.append(column_pvt)
    vars.append(column_df)
    
    return vars


    
    
    


    
    
    

    
    
