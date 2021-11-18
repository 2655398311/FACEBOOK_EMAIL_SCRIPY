
import re
import os
import json
import warnings
import requests
import numpy as np
import pandas as pd
from lxml import etree
from functools import reduce
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import matplotlib.font_manager as font_manager
warnings.filterwarnings('ignore')


def mysql_engine():
    user ='root'
    passwd ='root'
    host ='127.0.0.1'
    port ='3306'
    dbname1 = 'price_django'
    engine2 = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"%(user,passwd,host,port,dbname1))
    return engine2


#百度百科 参赛选手名单
def request_crawl(url):
    headers = {
        'cookie': 'zhishiTopicRequestTime=1617257185068; BAIKE_SHITONG=%7B%22data%22%3A%22fcb2c4b66285d3ee390c799bb7001ee1e51f8a45f46bef2576c8754d346e3e28406b51c05f3239f844f8ec4ac3fce31d2c63f630dcb90ead54546e238e287cfa6bd55344def0313856f1da52b5e17db8edb28621ebe9e229e70f82d8e67d4ddff12142d862f17136f45f0d248cb80f1fdf427927db528bd42705e98f8fcba91f%22%2C%22key_id%22%3A%2210%22%2C%22sign%22%3A%22d0a3ddb7%22%7D; BIDUPSID=5BC6C3BD539E54491DB458A10A6DE4E7; PSTM=1616654778; BAIDUID=5BC6C3BD539E54491BC8E48442A58E29:FG=1; __yjs_duid=1_4b37e58f0fffc294802743709fac7f8b1616655488776; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; BDSFRCVID_BFESS=xjkOJeC624jf-WTeD9KXbiGCofqF6B5TH6aoLZD4OgmTxPTVar9iEG0P8x8g0KubmUf4ogKKKgOTHICF_2uxOjjg8UtVJeC6EG0Ptf8g0f5; H_PS_PSSID=33819_33751_33344_31660_33691_33392_26350; BDRCVFR[S4-dAuiWMmn]=I67x6TjHwwYf0; delPer=0; PSINO=3; Hm_lvt_55b574651fcae74b0a9f1cf9c8d7c93a=1617245569; pcrightad16267=showed; ab_sr=1.0.0_ZWNiMTNjMmU5OWU0ZTNkOWY2NzIyOTU2YTQ3YWY3OWIxODUxYzUyNjFiZjI0OTFhMjMwOWY3NDc2Y2EyNDNiOTFiNjYyNDc3NjJhNzJkNTU5ZjAxZTlkMTQ3ZjgzMTRm; Hm_lpvt_55b574651fcae74b0a9f1cf9c8d7c93a=1617257263',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'etag': '5a3914c6-16bb3',
        'expires': 'Tue, 09 Mar 2021 03:46:28 GMT',
        'server': 'Tengine',
        'vary': 'Accept-Encoding'
    }

    response = requests.get(url=url,headers = headers)
    wb_data = response.text
    # print(wb_data)
    html = etree.HTML(wb_data.encode('utf-8'))
    data_list = html.xpath('//tr//td')
    result_data = []
    for i in data_list:
        actor_name = i.xpath(".//div[@class='para'][1]//b//a//text()")
        if len(actor_name) == 0:
            pass
        else:
            actor_name = actor_name
            result_dict = {"actor_name":''.join(actor_name)}
            # result_data.append(result_dict)
        actor_url = i.xpath('.//div[@class="para"][1]//b//a//@href')
        if len(actor_url) == 0:
            pass
        else:

            result_dict.update({"actor_url":''.join(actor_url)})
        actor_desc = i.xpath('.//div[@class="para"][2]//text()')
        if len(actor_desc) == 0 or len(actor_desc)>1:
            pass
        else:
            actor_desc = ''.join(actor_desc)
            result_dict.update({"actor_desc":actor_desc})
            # result_data.append(result_dict)

        magnum_opus = i.xpath('.//div[@class="para"][3]//text()')
        if len(magnum_opus) == 0 or ''.join(magnum_opus)=='':
            pass
        else:
            magnum_opus = ''.join(magnum_opus[1:])
            result_dict.update({"magnum_opus":magnum_opus})
            # result_data.append(result_dict)

        pic_url = i.xpath('.//div[@class="para"][4]//div//a//@href')
        if len(pic_url) == 0:
            pass
        else:
            pic_url = ''.join(pic_url)
            result_dict.update({"pic_url":pic_url})
            result_data.append(result_dict)
    return result_data

#保存图片 自动创建文件夹
def down_save_pic(name,pic_urls):
    '''
    根据图片链接列表pic_urls, 下载所有图片，保存在以name命名的文件夹中,
    '''
    path = 'work/'+'pics/'+name+'/'
    if not os.path.exists(path):
      os.makedirs(path)

    for i, pic_url in enumerate(pic_urls):
        try:
            pic = requests.get(pic_url, timeout=15)
            string = str(i + 1) + '.jpg'
            with open(path+string, 'wb') as f:
                f.write(pic.content)
                # print('成功下载第%s张图片: %s' % (str(i + 1), str(pic
                # _url)))
        except Exception as e:
            # print('下载第%s张图片时失败: %s' % (str(i + 1), str(pic_url)))
            print(e)
            continue

#参赛选手详情页抓取
def detail_request_crawl(detail_url,name):
    headers = {
                'cookie': 'zhishiTopicRequestTime=1617257185068; BAIKE_SHITONG=%7B%22data%22%3A%22fcb2c4b66285d3ee390c799bb7001ee1e51f8a45f46bef2576c8754d346e3e28406b51c05f3239f844f8ec4ac3fce31d2c63f630dcb90ead54546e238e287cfa6bd55344def0313856f1da52b5e17db8edb28621ebe9e229e70f82d8e67d4ddff12142d862f17136f45f0d248cb80f1fdf427927db528bd42705e98f8fcba91f%22%2C%22key_id%22%3A%2210%22%2C%22sign%22%3A%22d0a3ddb7%22%7D; BIDUPSID=5BC6C3BD539E54491DB458A10A6DE4E7; PSTM=1616654778; BAIDUID=5BC6C3BD539E54491BC8E48442A58E29:FG=1; __yjs_duid=1_4b37e58f0fffc294802743709fac7f8b1616655488776; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; BDSFRCVID_BFESS=xjkOJeC624jf-WTeD9KXbiGCofqF6B5TH6aoLZD4OgmTxPTVar9iEG0P8x8g0KubmUf4ogKKKgOTHICF_2uxOjjg8UtVJeC6EG0Ptf8g0f5; H_PS_PSSID=33819_33751_33344_31660_33691_33392_26350; BDRCVFR[S4-dAuiWMmn]=I67x6TjHwwYf0; delPer=0; PSINO=3; Hm_lvt_55b574651fcae74b0a9f1cf9c8d7c93a=1617245569; pcrightad16267=showed; ab_sr=1.0.0_ZWNiMTNjMmU5OWU0ZTNkOWY2NzIyOTU2YTQ3YWY3OWIxODUxYzUyNjFiZjI0OTFhMjMwOWY3NDc2Y2EyNDNiOTFiNjYyNDc3NjJhNzJkNTU5ZjAxZTlkMTQ3ZjgzMTRm; Hm_lpvt_55b574651fcae74b0a9f1cf9c8d7c93a=1617257263',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
                'etag': '5a3914c6-16bb3',
                'expires': 'Tue, 09 Mar 2021 03:46:28 GMT',
                'server': 'Tengine',
                'vary': 'Accept-Encoding'
            }
    response = requests.get(detail_url, headers=headers)
    # 将一段文档传入BeautifulSoup的构造方法,就能得到一个文档的对象
    bs = BeautifulSoup(response.text, 'lxml')
    # 获取选手的民族、星座、血型、体重等信息
    base_info_div = bs.find('div', {'class': 'basic-info cmn-clearfix'})
    dls = base_info_div.find_all('dl')
    star_infos = []
    star_info = {}
    for dl in dls:
        dts = dl.find_all('dt')
        for dt in dts:
            if "".join(str(dt.text).split()) == '民族':
                star_info['nation'] = dt.find_next('dd').text
            if "".join(str(dt.text).split()) == '星座':
                star_info['constellation'] = dt.find_next('dd').text
            if "".join(str(dt.text).split()) == '血型':
                star_info['blood_type'] = dt.find_next('dd').text
            if "".join(str(dt.text).split()) == '身高':
                height_str = str(dt.find_next('dd').text)
                star_info['height'] = str(height_str[0:height_str.rfind('cm')]).replace("\n", "")
            if "".join(str(dt.text).split()) == '体重':
                star_info['weight'] = str(dt.find_next('dd').text).replace("\n", "")
            if "".join(str(dt.text).split()) == '出生日期':
                birth_day_str = str(dt.find_next('dd').text).replace("\n", "")
                # print(birth_day_str)
                # if '年' in birth_day_str:
                star_info['birth_day'] = birth_day_str
    star_infos.append(star_info)

    if bs.select('.summary-pic a'):
        try:
            pic_list_url = bs.select('.summary-pic a')[0].get('href')
            pic_list_url = 'https://baike.baidu.com' + pic_list_url
        except:
            pass
        # print(pic_list_url)
        # 向选手图片列表页面发送http get请求
    # print(pic_list_url)
    try:
        pic_list_response = requests.get(pic_list_url, headers=headers)

        # 对选手图片列表页面进行解析，获取所有图片链接
        baa = BeautifulSoup(pic_list_response.text, 'lxml')
        pic_list_html = baa.select('.pic-list img ')
        pic_urls = []
        for pic_html in pic_list_html:
            pic_url = pic_html.get('src')
            pic_urls.append(pic_url)
        down_save_pic(name, pic_urls)


    except:
        pass

    return star_infos

#list中字典元素去重
def list_dict_duplicate_removal(data_list):

    run_function = lambda x, y: x if y in x else x + [y]
    return reduce(run_function, [[], ] + data_list)

#爬虫主程序运行函数   Main方法
def main_execute_crawl():
    data = request_crawl(url='https://baike.baidu.com/item/%E4%B9%98%E9%A3%8E%E7%A0%B4%E6%B5%AA%E7%9A%84%E5%A7%90%E5%A7%90/49998987?fr=aladdin')
    data_frame = pd.DataFrame(data)
    data_list = list_dict_duplicate_removal(data_list=data)
    aa = []
    for i in data_list:
        url = "https://baike.baidu.com"+i["actor_url"]
        data_result = detail_request_crawl(detail_url=url,name=i["actor_name"])

        #去除字典中的空格
        for i in data_frame.iterrows():
            for ab in data_result:
                for key, value in ab.items():
                    ab[key] = value.strip()
                ab.update({"actor_desc":i[1]["actor_desc"],"actor_name":i[1]["actor_name"],"actor_url":i[1]["actor_url"],"magnum_opus":i[1]["magnum_opus"],"pic_url":i[1]["pic_url"]})
                data_frame = pd.DataFrame([ab])
                print(data_frame)
                # pd.io.sql.to_sql(data_frame, 'baidu_detail', mysql_engine(), schema='price_django', if_exists='append', index=False)

if __name__ == '__main__':
    # # data_analysis()
    main_execute_crawl()

    # detail_request_crawl(detail_url='https://baike.baidu.com/item/%E7%99%BD%E5%86%B0/10967')
    # aa = request_crawl(url="s")

    # detail_request_crawl(url='')
