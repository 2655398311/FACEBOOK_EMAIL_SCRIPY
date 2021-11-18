

import re
import json
import redis
import warnings
import requests
import itertools
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')



def mysql_engine():

    user ='root'
    passwd ='123456'
    host ='10.228.83.123'
    port ='13306'
    dbname1 = 'user_huaxiang'
    engine2 = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"%(user,passwd,host,port,dbname1))
    return engine2



def douban_crawl(url):

    headers = {
        'cookie': 'll="118172"; bid=keKNz7B6iWQ; __yadk_uid=LvNf6bieWO30AbXra7tfrZ0spqNgRU2p; _vwo_uuid_v2=D9A6EE4EBA02DE98808915FCE983BE556|460831d1a961bfd92ae6bac66c8f5bac; ap_v=0,6.0; __utmc=30149280; __utmz=30149280.1618641943.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmc=223695111; __utmz=223695111.1618641943.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1618644736%2C%22https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3Dufk0pxDtmO16x1iSLlDQ4xNsu0h8wHvwwOstxhpUPY2kxrq7Nw5kCp5xYHAP5cv5%26wd%3D%26eqid%3D95a4afe10001649e00000005607a843b%22%5D; _pk_ses.100001.4cf6=*; __utma=30149280.617296376.1618641943.1618641943.1618644736.2; __utmb=30149280.0.10.1618644736; __utma=223695111.569142583.1618641943.1618641943.1618644736.2; __utmb=223695111.0.10.1618644736; dbcl2="224779965:4OVqptxFXoQ"; ck=pr18; push_noty_num=0; push_doumail_num=0; _pk_id.100001.4cf6=f2c7998398a2f937.1618627082.3.1618645514.1618641942.',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'etag': '5a3914c6-16bb3',
        'expires': 'Tue, 09 Mar 2021 03:46:28 GMT',
        'server': 'Tengine',
        'vary': 'Accept-Encoding'
    }

    response = requests.get(url=url,headers = headers)
    wb_data = response.text
    html = etree.HTML(wb_data.encode('utf-8'))
    data_list = html.xpath('//div[@class="s"]//div[@class="screening-bd"]//ul[@class="ui-slide-content"]')
    data_info_list = []
    for i in data_list:

        detail_url = i.xpath('./li[@class="ui-slide-item s"]//ul//li[@class="poster"]//a//@href')
        title = i.xpath('./li[@class="ui-slide-item s"]//ul//li[@class="title"]//a//text()')
        movie_score = i.xpath('./li[@class="ui-slide-item s"]//ul//li[@class="rating"]//span//text()')
        for de,tit,score in itertools.zip_longest(detail_url,title,movie_score):
            data_info_list.append({"detail_url":de,"title":tit,'movie_score':score})
    return data_info_list

def crawl_movie_detail(url):

    headers = {
        'cookie': 'll="118172"; bid=keKNz7B6iWQ; __yadk_uid=LvNf6bieWO30AbXra7tfrZ0spqNgRU2p; _vwo_uuid_v2=D9A6EE4EBA02DE98808915FCE983BE556|460831d1a961bfd92ae6bac66c8f5bac; ap_v=0,6.0; __utmc=30149280; __utmz=30149280.1618641943.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmc=223695111; __utmz=223695111.1618641943.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1618644736%2C%22https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3Dufk0pxDtmO16x1iSLlDQ4xNsu0h8wHvwwOstxhpUPY2kxrq7Nw5kCp5xYHAP5cv5%26wd%3D%26eqid%3D95a4afe10001649e00000005607a843b%22%5D; _pk_ses.100001.4cf6=*; __utma=30149280.617296376.1618641943.1618641943.1618644736.2; __utmb=30149280.0.10.1618644736; __utma=223695111.569142583.1618641943.1618641943.1618644736.2; __utmb=223695111.0.10.1618644736; dbcl2="224779965:4OVqptxFXoQ"; ck=pr18; push_noty_num=0; push_doumail_num=0; _pk_id.100001.4cf6=f2c7998398a2f937.1618627082.3.1618645514.1618641942.',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'etag': '5a3914c6-16bb3',
        'expires': 'Tue, 09 Mar 2021 03:46:28 GMT',
        'server': 'Tengine',
        'vary': 'Accept-Encoding'
    }
    response = requests.get(url=url, headers=headers)
    wb_data = response.text
    html = etree.HTML(wb_data.encode('utf-8'))
    desc = html.xpath('//div[@class="subjectwrap clearfix"]//div[@class="subject clearfix"]//div[@id="info"]//span//span[@class="pl"]//text()')
    desc1 = html.xpath('//div[@class="subjectwrap clearfix"]//div[@class="subject clearfix"]//div[@id="info"]//span//span[@class="attrs"]//text()')
    #编剧
    desc2 = html.xpath('//div[@class="subjectwrap clearfix"]//div[@class="subject clearfix"]//div[@id="info"]//span[2]//span[@class="pl"]//text()')
    #编剧结果
    desc3 = html.xpath('//div[@class="subjectwrap clearfix"]//div[@class="subject clearfix"]//div[@id="info"]//span[2]//span[@class="attrs"]//text()')
    #主演
    desc4 = html.xpath('//div[@class="subjectwrap clearfix"]//div[@class="subject clearfix"]//div[@id="info"]//span[3]//span[@class="pl"]//text()')
    #主演结果
    desc5 = html.xpath('//div[@class="subjectwrap clearfix"]//div[@class="subject clearfix"]//div[@id="info"]//span[3]//span[@class="attrs"]//text()')
    desc6 = html.xpath('//div[@class="subjectwrap clearfix"]//div[@class="subject clearfix"]//div[@id="info"]//span[@class="pl"]//text()')
    desc7 = html.xpath('//div[@class="subjectwrap clearfix"]//div[@class="subject clearfix"]//div[@id="info"]//span[5]//text()')
    movie_desc = html.xpath('//*[@id="link-report"]//span//text()')
    movie_desc = [x.strip() for x in movie_desc]
    all_actor_url = html.xpath('//*[@id="celebrities"]/h2//span//a//@href')
    #用于字符串拼接  链接url拼接
    #https://movie.douban.com/subject/35158160/celebrities
    all_actor_url = "https://movie.douban.com"+''.join(all_actor_url)

    dict_result = [{"scriptwriter": ''.join(desc3), "c": ''.join(desc5),"movie_type":''.join(desc7),"movie_desc":''.join(movie_desc),"all_actor_url":all_actor_url}]

    return dict_result

def crawl_movie_actor(url):
    headers = {
        'cookie': 'll="118172"; bid=keKNz7B6iWQ; __yadk_uid=LvNf6bieWO30AbXra7tfrZ0spqNgRU2p; _vwo_uuid_v2=D9A6EE4EBA02DE98808915FCE983BE556|460831d1a961bfd92ae6bac66c8f5bac; ap_v=0,6.0; __utmc=30149280; __utmz=30149280.1618641943.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmc=223695111; __utmz=223695111.1618641943.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1618644736%2C%22https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3Dufk0pxDtmO16x1iSLlDQ4xNsu0h8wHvwwOstxhpUPY2kxrq7Nw5kCp5xYHAP5cv5%26wd%3D%26eqid%3D95a4afe10001649e00000005607a843b%22%5D; _pk_ses.100001.4cf6=*; __utma=30149280.617296376.1618641943.1618641943.1618644736.2; __utmb=30149280.0.10.1618644736; __utma=223695111.569142583.1618641943.1618641943.1618644736.2; __utmb=223695111.0.10.1618644736; dbcl2="224779965:4OVqptxFXoQ"; ck=pr18; push_noty_num=0; push_doumail_num=0; _pk_id.100001.4cf6=f2c7998398a2f937.1618627082.3.1618645514.1618641942.',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'etag': '5a3914c6-16bb3',
        'expires': 'Tue, 09 Mar 2021 03:46:28 GMT',
        'server': 'Tengine',
        'vary': 'Accept-Encoding'
    }
    response = requests.get(url=url, headers=headers)
    wb_data = response.text
    html = etree.HTML(wb_data.encode('utf-8'))
    data_list = html.xpath('//div[@class="grid-16-8 clearfix"]//div[@class="article"]//div[@class="mod-bd celebrities"]')
    data_result = []
    for i in data_list:
        actor_name_list = i.xpath('./div[@class="list-wrapper"]//ul//li//div[@class="info"]//span[@class="name"]//a//text()')
        actor_name_url_list = i.xpath('./div[@class="list-wrapper"]//ul//li//div[@class="info"]//span[@class="name"]//a//@href')
        director_desc_list = i.xpath('./div[@class="list-wrapper"]//ul//li//div[@class="info"]//span[@class="role"]//text()')
        magnum_opus_list = i.xpath('./div[@class="list-wrapper"]//ul//li//div[@class="info"]//span[@class="works"]//text()')
        s = [x.strip() for x in magnum_opus_list]
        str_default_list = '/'.join(s).split("代表作：")
        #代表作品列表
        magnum_opus_list_list = [x.strip() for x in str_default_list if x.strip() != '']
        for actor_name,actor_name_url,director_desc,magnum_opus_list in itertools.zip_longest(actor_name_list,actor_name_url_list,director_desc_list,magnum_opus_list_list):
            data_result.append({"actor_name":actor_name,"actor_name_url":actor_name_url,"director_desc":director_desc,"magnum_opus":magnum_opus_list})
    return data_result

def crawl_movie_actor_result():
    data_list = douban_crawl(url="https://movie.douban.com/")
    dict_aa_list = []
    for i in data_list:
        data = crawl_movie_detail(url=i["detail_url"])
        for j in data:
            i.update(j)
            data_1 = crawl_movie_actor(j["all_actor_url"])
            for a in data_1:
                i.update(a)
        dict_aa_list.append(i)
    data_list_type = pd.DataFrame(dict_aa_list)
    data_list_type["actor_name"] = data_list_type['actor_name'].astype("str")
    data_list_type["c"] = data_list_type['c'].astype("str")
    print(data_list_type)
    # data_list_type.to_csv("douban.csv",mode='a')
    #入库
    pd.io.sql.to_sql(data_list_type, 'douban_movie', mysql_engine(), schema='user_huaxiang', if_exists='append', index=False)


if __name__ == '__main__':

    #调用最终启动爬虫函数  main函数
    crawl_movie_actor_result()
    # crawl_movie_actor_detail(url='https://movie.douban.com/celebrity/1398700')
    # crawl_movie_actor(url='https://movie.douban.com/subject/35158160/celebrities')
    # crawl_movie_detail(url="https://movie.douban.com/subject/26382960/?from=showing")
    # crawl_movie_detail(url="https://movie.douban.com/subject/35158160/?from=showing")
    # zhihu_crawl(url="https://movie.douban.com/")