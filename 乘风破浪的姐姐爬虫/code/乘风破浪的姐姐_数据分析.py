
import json
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import matplotlib.font_manager as font_manager
from matplotlib.ticker import FormatStrFormatter

warnings.filterwarnings("ignore")

def mysql_engine():
    user ='root'
    passwd ='12345678'
    host ='127.0.0.1'
    port ='3306'
    dbname1 = 'baidu_data'
    engine2 = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"%(user,passwd,host,port,dbname1))
    return engine2

def data_analysis():
    df = pd.read_sql("select * from price_django.baidu_detail",mysql_engine())
    # print(df)
    df = df.dropna()
    # df = df["birth_day"]
    df["birth_day"] = df["birth_day"].apply(lambda x:x[:4])
    df = df[df['birth_day'].map(len)==4]
    df = df[~df["birth_day"].isin(["一说19", "3月22"])]
    grouped = df['actor_name'].groupby(df['birth_day'])
    s = grouped.count()
    birth_days_list = s.index
    count_list = s.values
    print(count_list)
    plt.figure(figsize=(15, 8))
    plt.bar(range(len(count_list)), count_list, color='r', tick_label=birth_days_list,
            facecolor='#9999ff', edgecolor='white')
    # 这里是调节横坐标的倾斜度，rotation是度数，以及设置刻度字体大小
    plt.xticks(rotation=45, fontsize=20)
    plt.yticks(fontsize=20)
    plt.legend()
    plt.title('age of girls', fontsize=24)
    plt.savefig('work/bar_result01_age.jpg')
    plt.show()


def wieght_analysis():

    df = pd.read_sql("select * from price_django.baidu_detail",mysql_engine())
    data_list = [df.ix[i].to_dict() for i in df.index.values]
    weights = []
    for i in data_list:
        if 'weight' in dict(i).keys():
            if i['weight'] == None:
                pass
            else:
                weight = float(i["weight"][0:2])
                weights.append(weight)
    size_list = []
    count_list = []
    size1 = 0
    size2 = 0
    size3 = 0
    size4 = 0
    for weight in weights:
        if weight <= 45:
            size1 += 1
        elif 45 < weight <= 50:
            size2 += 1
        elif 50 < weight <= 55:
            size3 += 1
        else:
            size4 += 1
    labels = '<=45kg', '45~50kg', '50~55kg', '>55kg'
    sizes = [size1, size2, size3, size4]
    explode = (0.2, 0.1, 0, 0)
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
            shadow=True)
    ax1.axis('equal')
    plt.savefig('pie_result01_weight.jpg')
    plt.show()

def height_analysis():
    df = pd.read_sql("select * from price_django.baidu_detail", mysql_engine())
    data_list = [df.ix[i].to_dict() for i in df.index.values]
    heights = []
    for i in data_list:
        if 'height' in dict(i).keys():
            if i['height'] == None:
                pass
            else:
                height = float(i["height"][0:3])
                heights.append(height)
    size_list = []
    count_list = []

    size1 = 0
    size2 = 0
    size3 = 0
    size4 = 0

    for height in heights:
        if height <= 162:
            size1 += 1
        elif 162 < height <= 166:
            size2 += 1
        elif 166 < height <= 170:
            size3 += 1
        else:
            size4 += 1

    labels = '<=162cm', '162~166cm', '166~170cm', '>170cm'

    sizes = [size1, size2, size3, size4]
    explode = (0.2, 0.1, 0, 0)
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
            shadow=True)
    ax1.axis('equal')
    plt.savefig('work/height_result.jpg')
    plt.title('Height distribution', fontsize=14)
    plt.show()


def weight_height_analysis():

    weights = []

    heights = []

    counts = []

    df = pd.read_sql("select * from baidu_data.baidu_detail", mysql_engine())
    data_list = [df.ix[i].to_dict() for i in df.index.values]
    for i in data_list:
        if 'weight' in dict(i).keys():
            if i['weight'] == None:
                pass
            else:
                weight = float(i["weight"][0:2])
                weights.append(weight)

        if 'height' in dict(i).keys():
            if i['height'] == None:
                pass
            else:
                height = float(i["height"][0:3])
                heights.append(height)
    plt.scatter(heights[0:23], weights, alpha=0.6)  # 绘制散点图，透明度为0.6（这样颜色浅一点，比较好看）
    plt.title("figure of weights and heights")
    plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%d kg'))
    plt.gca().xaxis.set_major_formatter(FormatStrFormatter('%d cm'))
    plt.savefig('work/hw_result.jpg')
    plt.show()



if __name__ == '__main__':
    data_analysis()
    wieght_analysis()
    height_analysis()
    weight_height_analysis()
