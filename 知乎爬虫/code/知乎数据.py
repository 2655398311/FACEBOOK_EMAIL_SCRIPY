#encoding:utf-8
"""
@project=企查查
@file=知乎数据
@author=hjfan
@create_time:2021/8/28 10:15
"""


import os
import xlsxwriter
import pandas as pd

df = pd.read_excel(r"D:\企查查\biaoge\sky_cat.xlsx")

path = r"D:\企查查\yiyue_pic_aa"
pics = os.listdir(path)


book = xlsxwriter.Workbook(r"D:\企查查\biaoge\sky_cat.xlsx")
sheet = book.add_worksheet("pic")

sheet.write("I1", "pic_picture")
sheet.write_column(1, 0, df.taobao_goods_id.values.tolist()) #昵称放在第一列


cell_width = 25
cell_height = 120
sheet.set_column("B:B", cell_width) # 设置单元格列宽

