import os
from functions import load_workbook, load_vars, load_pivot_table, clean_filter, generator_dataframe, clean_dataframe

def main():
    # Start Processing
    print('********Start - Process********')  
    try:
        # Step 01 - Load workbook 
        
        print('Step 1 - Loading workbook')
        wb = load_workbook()

        # Step 02 - Define Variables
        print('Step 2 - Define Variables')
        vars1 = load_vars('pvt1')
        vars2 = load_vars('pvt2')


        # Step 03 - Table Pivot
        print('Step 3 - Generation datasets')

        # Path the storage 
        path = os.path.dirname(os.path.abspath(__file__)) + '/dados_win32/'
        file_derivative = path + 'dataset_derivative.csv'
        file_diesel = path + 'dataset_diesel.csv'

        # Create paht case not exist
        if not os.path.exists(path):
            os.makedirs(path)
            
            
        # Step 3.1 - Sales of oil derivative fuels by UF and product
        print('Step 3.2 - Extratct data (Sales of oil derivative fuels by UF and product)')
        filters = vars1[0]
        ranges_pvt1 = vars1[1]
        column_pvt1 = vars1[2]
        columns_df1 = vars1[3]

        # Load pivot table
        pvtTable1 = load_pivot_table(wb, ranges_pvt1[0], ranges_pvt1[1])
        # Clean filter - UN. DA FEDERAÇÃO
        clean_filter(pvtTable1, filters[0])
        # Clean filter - PRODUTO
        clean_filter(pvtTable1, filters[1])
        # Genaration dataset
        df1 = generator_dataframe(pvtTable1, columns_df1, column_pvt1, filters[0], filters[1])
        df_melt1 = df1.melt(id_vars=["uf", "produto", 'mes'], var_name="ano", value_name="volume")
        df_deravative = clean_dataframe(df_melt1)

        # Step 3.2 - Sales of diesel by UF and type
        print('Step 3.3 - Extratct data (Sales of diesel by UF and type)', end='\n\n')
        filters = vars2[0]
        ranges_pvt2 = vars2[1]
        column_pvt2 = vars2[2]
        columns_df2 = vars2[3]
            
        pvtTable2 = load_pivot_table(wb, ranges_pvt2[0], ranges_pvt2[1])
        # Clean filter - UN. DA FEDERAÇÃO
        clean_filter(pvtTable2, filters[0])
        # Clean filter - PRODUTO
        clean_filter(pvtTable2, filters[1])
        # Genaration dataset
        df2 = generator_dataframe(pvtTable2, columns_df2, column_pvt2, filters[0], filters[1])
        df_melt2 = df2.melt(id_vars=["uf", "produto", 'mes'], var_name="ano", value_name="volume")
        df_diesel = clean_dataframe(df_melt2)

        print('********Start - Create File Datasets********')
        df_deravative.to_csv(file_derivative, sep = ';', index=False)
        print('Create Dataset - DERIVATIVES') 
        df_diesel.to_csv(file_diesel, sep = ';', index=False)
        print('Create Dataset - DIESEL') 
            
        print('********End - Create File Datasets********', end='\n\n')
    except:
        print("Process failed")
        print('********End - Process********', end='\n\n')


# Execute process
if __name__ == '__main__':
    main()