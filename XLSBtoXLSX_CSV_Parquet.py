import os
import pandas as pd
import shutil

#use you source and target databricks mount path or local path
path=r''
target_path=r''

def toExcel(path,target_path):
  for file in os.listdir(path):
      if file.endswith('.xlsb'):
          fullpath=os.path.join(path,file)
          print(fullpath)
          #"pip install pyxlsb" engine for reading the excel binary file
          sheets=pd.ExcelFile(fullpath,engine='pyxlsb')
          sheet_list=sheets.sheet_names
          output_file_name=file.split('.')[0]+'.xlsx'
          print(output_file_name)
          writer = pd.ExcelWriter(output_file_name,engine='xlsxwriter')
          for list_of_sheets in sheet_list:
            src_data=pd.read_excel(fullpath,sheet_name=list_of_sheets,engine='pyxlsb')
            print(src_data)
            print(os.getcwd())
            final_path=target_path+f'/{output_file_name}'
            src_data.to_excel(writer,sheet_name=list_of_sheets,header=True,index=False)
            
          
          writer.save()
          shutil.move(output_file_name,final_path)
          #os.remove(fullpath)
          
def toCsv(path,target_path):
  for file in os.listdir(path):
      if file.endswith('.xlsb'):
          fullpath=os.path.join(path,file)
          print(fullpath)
          #"pip install pyxlsb" engine for reading the excel binary file
          sheets=pd.ExcelFile(fullpath,engine='pyxlsb')
          sheet_list=sheets.sheet_names
          

          for list_of_sheets in sheet_list:
            print(list_of_sheets)
            src_data=pd.read_excel(fullpath,sheet_name=list_of_sheets,engine='pyxlsb')
            print(src_data)
            print(os.getcwd())
            output_file_name=file.split('.')[0]+f'_{list_of_sheets}'+'.csv'
            print(output_file_name)
            final_path=target_path+f'/{output_file_name}'            
            src_data.to_csv(output_file_name,header=True,index=False)
            shutil.move(output_file_name,final_path)
            
            
def toParquet(path,target_path):
  for file in os.listdir(path):
      if file.endswith('.xlsb'):
          fullpath=os.path.join(path,file)
          print(fullpath)
          #"pip install pyxlsb" engine for reading the excel binary file
          sheets=pd.ExcelFile(fullpath,engine='pyxlsb')
          sheet_list=sheets.sheet_names          

          for list_of_sheets in sheet_list:
            print(list_of_sheets)
            src_data=pd.read_excel(fullpath,sheet_name=list_of_sheets,engine='pyxlsb')
            output_file_name=file.split('.')[0]+f'_{list_of_sheets}'+'.parquet'
            print(output_file_name)
            final_path=target_path+f'/{output_file_name}'            
            src_data.to_parquet(output_file_name,index=False)
            shutil.move(output_file_name,final_path)
            
          
#FunctiontoCall
toParquet(path,target_path)
       
