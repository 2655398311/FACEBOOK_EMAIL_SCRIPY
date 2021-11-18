import re
import json
import redis
import warnings
import requests
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')

#配置redis 实现分布式

redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
redis_conn = redis.Redis(connection_pool=redis_pool)


def mysql_engine():
    user ='root'
    passwd ='123456'
    host ='10.228.83.123'
    port ='13306'
    dbname1 = 'user_huaxiang'
    engine2 = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"%(user,passwd,host,port,dbname1))
    return engine2

def zhihu_crawl(url):

    headers = {
        'cookie': 'SESSIONID=1onFk2N4GxONhGQucLTF1gHjxautyXP67KoU5r9jSLV; osd=W10XB0mVHD0UQyBEP5ibKYfsf8Em0VROSjh4EHT3Iw5ac2ktDfUMbXlEK0U_R9o1Rhlsr4EnxEP7ozse6S6fKPQ=; JOID=Vl8QAEmYHjoTQy1GOJ-bJIXreMEr01NJSjV6F3P3LgxddGkgD_ILbXRGLEI_StgyQRlhrYYgxE75pDwe5CyYL_Q=; _zap=c7682c52-360f-44a9-95ed-e6b787d40a11; d_c0="ADAd15xBgRKPTl3ZxfbQPbd4LbpSdxtQ1f8=|1610691888"; _xsrf=9584bb49-f417-40c0-83d0-e40b557b9b9a; captcha_session_v2="2|1:0|10:1615786158|18:captcha_session_v2|88:R1Z3WHp0TWIvRU1jOW5aK0N5bVZOYkVra2FCZUt1eWhRZzJUWWF1b1p2Ukt0bUhRTGRHKzFBejRnc1M1N1pMSg==|5f31282d696351ea5c7bcdac15e25e894c518e0e6d0c41da98d933d4bd817dad"; captcha_ticket_v2="2|1:0|10:1615786169|17:captcha_ticket_v2|228:eyJhcHBpZCI6IjIwMTIwMzEzMTQiLCJyZXQiOjAsInRpY2tldCI6InQwMzdmV2JsWW9PNDJBYWIyNGhZdm80V05TYUFGbjBXaFhhS2QyMGRtVDUxeG9Ic3pJT2xWaFh3VFZHMXo0UVFsN25Tek1aRXJib0hzRGYyN0dwcm4yNnBHc2FxbzRvMjNMZTZoWUdqeEYzQnBnKiIsInJhbmRzdHIiOiJAWmFIIn0=|1e0242f221a3e48eb237077e18a0dad1c850a951221ec7d0d50dc702feb51de7"; tst=h; tshl=; q_c1=518151e75d3b493c975cc84440150ffd|1615786198000|1615786198000; SESSIONID=HOuXpTSMi34H6Be5Smw3AnckD67BWs9orHKeNyDSu6q; JOID=VFgTCksDkWj0OopTIQwRd2-T19EzTdgSpUzYBmNspFqzBc4wEgCVPps6ilonHGm1lNgFLav83xXWgLvkdqNbvg0=; osd=VFwUBU0DlW_7PIpXJgMXd2uU2NczSd8do0zcAWxqpF60CsgwFgeaOJs-jVUhHG2ym94FKazz2RXSh7TidqdcsQs=; l_n_c=1; n_c=1; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1615517406,1615786121,1615858909,1615884629; r_cap_id="NmE0NjNhYTA0ZmQ5NDczNWI5NjY1OGMwMDdhNDIzYjg=|1615947684|e1a182240128a1a1ddbbd335d078eee69b574276"; cap_id="NDk2MzNlNDJjMDg0NDAzMmJhNjcyMmI4YjBiNGFkNDY=|1615947684|82b4af491b95d5a12620b2d9a8a04e14db541efd"; l_cap_id="M2I2YmE2ODcxMGUxNDEzNWJiNGE2ZDVjYWVjZTExNzk=|1615947684|ce768d645626e54a3efec7b20e2667fc15fe215d"; atoken=43_i2T0BLV3XnlRvYLt4H_rlhQ_W_bQ-JetGBYDm8_wFp4gEKq7rDyqsFIy9lhd2L_OdyRpaI-qesejIVKkwLcG8hpSLDfDWsvA3o8IE8OD_tg; atoken_expired_in=7200; client_id="bzNwMi1qdTYtZm1qdGk5dURzQS1NVWs5WHNBVQ==|1615947693|3128d54e0737fc22342cb39326c5a393e0bcc1c6"; capsion_ticket="2|1:0|10:1615947693|14:capsion_ticket|44:OWFiZDI4MzZkZDQ3NDE4ZThiODE1M2ZjNDVhMDQxNGY=|f420e7cf3e349c6e23fdcc33850e38c69ed684abb4c6998f50ecebe1d2c53bbe"; z_c0="2|1:0|10:1615947713|4:z_c0|92:Mi4xRDNYaEdBQUFBQUFBTUIzWG5FR0JFaVlBQUFCZ0FsVk53TFUtWVFCRTI4MnVaRWVmYnpSZkp6RVlzUjVCM2ZsaGJ3|6e4dfb84ce68be84d71466624e755993d238e35cceb6596167907ec943f60743"; unlock_ticket="AFCXk8-qwhAmAAAAYAJVTchuUWCbVw4JQw7PCrawY__5XvczsbafzA=="; Hm_lpvt_98beee57fd2ef70ccdd5ca52b9740c49=1615947675; KLBRSID=4843ceb2c0de43091e0ff7c22eadca8c|1615947715|1615945435',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'etag': '5a3914c6-16bb3',
        'expires': 'Tue, 09 Mar 2021 03:46:28 GMT',
        'server': 'Tengine',
        'vary': 'Accept-Encoding'
    }

    response = requests.get(url=url,headers = headers)
    wb_data = response.text
    html = etree.HTML(wb_data.encode('utf-8'))
    all_data_list = html.xpath('//*[@id="TopstoryContent"]//div//div//div//section//div')
    item = []
    item1 = {}
    for i in all_data_list:
        hot_title = i.xpath("./a//h2//text()")
        if len(hot_title) == 0:
            pass
        else:
            hot_title = ''.join(hot_title)
            item1.update({"hot_title":hot_title})
            # item.append({"title":hot_title})
        article_hot = i.xpath('./div[@class="HotItem-metrics HotItem-metrics--bottom"]//text()')
        if len(article_hot) == 0:
            pass
        else:
            article_hot = article_hot[0]
            item1.update({"article_hot": article_hot})

        hot_title_url = i.xpath('./a//@href')
        if len(hot_title_url) == 0:
            pass
        else:
            hot_title_url = ''.join(hot_title_url)
            item1.update({"hot_title_url":hot_title_url})
            # item.append({"title":hot_title,"article_hot":article_hot,"hot_title_url":hot_title_url})
            item.append(item1)
    print(item)
    return item

def article_content_crawl(content_url):

    headers = {
        'cookie': 'SESSIONID=1onFk2N4GxONhGQucLTF1gHjxautyXP67KoU5r9jSLV; osd=W10XB0mVHD0UQyBEP5ibKYfsf8Em0VROSjh4EHT3Iw5ac2ktDfUMbXlEK0U_R9o1Rhlsr4EnxEP7ozse6S6fKPQ=; JOID=Vl8QAEmYHjoTQy1GOJ-bJIXreMEr01NJSjV6F3P3LgxddGkgD_ILbXRGLEI_StgyQRlhrYYgxE75pDwe5CyYL_Q=; _zap=c7682c52-360f-44a9-95ed-e6b787d40a11; d_c0="ADAd15xBgRKPTl3ZxfbQPbd4LbpSdxtQ1f8=|1610691888"; _xsrf=9584bb49-f417-40c0-83d0-e40b557b9b9a; captcha_session_v2="2|1:0|10:1615786158|18:captcha_session_v2|88:R1Z3WHp0TWIvRU1jOW5aK0N5bVZOYkVra2FCZUt1eWhRZzJUWWF1b1p2Ukt0bUhRTGRHKzFBejRnc1M1N1pMSg==|5f31282d696351ea5c7bcdac15e25e894c518e0e6d0c41da98d933d4bd817dad"; captcha_ticket_v2="2|1:0|10:1615786169|17:captcha_ticket_v2|228:eyJhcHBpZCI6IjIwMTIwMzEzMTQiLCJyZXQiOjAsInRpY2tldCI6InQwMzdmV2JsWW9PNDJBYWIyNGhZdm80V05TYUFGbjBXaFhhS2QyMGRtVDUxeG9Ic3pJT2xWaFh3VFZHMXo0UVFsN25Tek1aRXJib0hzRGYyN0dwcm4yNnBHc2FxbzRvMjNMZTZoWUdqeEYzQnBnKiIsInJhbmRzdHIiOiJAWmFIIn0=|1e0242f221a3e48eb237077e18a0dad1c850a951221ec7d0d50dc702feb51de7"; tst=h; tshl=; q_c1=518151e75d3b493c975cc84440150ffd|1615786198000|1615786198000; SESSIONID=HOuXpTSMi34H6Be5Smw3AnckD67BWs9orHKeNyDSu6q; JOID=VFgTCksDkWj0OopTIQwRd2-T19EzTdgSpUzYBmNspFqzBc4wEgCVPps6ilonHGm1lNgFLav83xXWgLvkdqNbvg0=; osd=VFwUBU0DlW_7PIpXJgMXd2uU2NczSd8do0zcAWxqpF60CsgwFgeaOJs-jVUhHG2ym94FKazz2RXSh7TidqdcsQs=; l_n_c=1; n_c=1; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1615517406,1615786121,1615858909,1615884629; r_cap_id="NmE0NjNhYTA0ZmQ5NDczNWI5NjY1OGMwMDdhNDIzYjg=|1615947684|e1a182240128a1a1ddbbd335d078eee69b574276"; cap_id="NDk2MzNlNDJjMDg0NDAzMmJhNjcyMmI4YjBiNGFkNDY=|1615947684|82b4af491b95d5a12620b2d9a8a04e14db541efd"; l_cap_id="M2I2YmE2ODcxMGUxNDEzNWJiNGE2ZDVjYWVjZTExNzk=|1615947684|ce768d645626e54a3efec7b20e2667fc15fe215d"; atoken=43_i2T0BLV3XnlRvYLt4H_rlhQ_W_bQ-JetGBYDm8_wFp4gEKq7rDyqsFIy9lhd2L_OdyRpaI-qesejIVKkwLcG8hpSLDfDWsvA3o8IE8OD_tg; atoken_expired_in=7200; client_id="bzNwMi1qdTYtZm1qdGk5dURzQS1NVWs5WHNBVQ==|1615947693|3128d54e0737fc22342cb39326c5a393e0bcc1c6"; capsion_ticket="2|1:0|10:1615947693|14:capsion_ticket|44:OWFiZDI4MzZkZDQ3NDE4ZThiODE1M2ZjNDVhMDQxNGY=|f420e7cf3e349c6e23fdcc33850e38c69ed684abb4c6998f50ecebe1d2c53bbe"; z_c0="2|1:0|10:1615947713|4:z_c0|92:Mi4xRDNYaEdBQUFBQUFBTUIzWG5FR0JFaVlBQUFCZ0FsVk53TFUtWVFCRTI4MnVaRWVmYnpSZkp6RVlzUjVCM2ZsaGJ3|6e4dfb84ce68be84d71466624e755993d238e35cceb6596167907ec943f60743"; unlock_ticket="AFCXk8-qwhAmAAAAYAJVTchuUWCbVw4JQw7PCrawY__5XvczsbafzA=="; Hm_lpvt_98beee57fd2ef70ccdd5ca52b9740c49=1615947675; KLBRSID=4843ceb2c0de43091e0ff7c22eadca8c|1615947715|1615945435',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'etag': '5a3914c6-16bb3',
        'expires': 'Tue, 09 Mar 2021 03:46:28 GMT',
        'server': 'Tengine',
        'vary': 'Accept-Encoding'
    }
    # href="//www.zhihu.com/people/ycy19980731"
    # href="//www.zhihu.com/people/fan-jing-shi-32"
    #href="//www.zhihu.com/people/nogirlnotalk"
    response = requests.get(url=content_url, headers=headers)
    wb_data = response.text
    html = etree.HTML(wb_data.encode('utf-8'))
    #用户信息提取
    data = re.findall('"author":{.*?}',wb_data)
    #热榜内容提取
    data1 = re.findall('"content":".*?"',wb_data)
    #热榜新闻详情页跳转链接提取
    data2 = re.findall('target="_blank" href=".*?"',wb_data)
    #详情页跳转链接关键词拼接提取
    userinfo_url = re.findall('"urlToken":".*?"',wb_data)
    item_userinfo = []
    #将提取出来的三个列表进行打包提取
    for i,a,c in zip(data,data1,userinfo_url):
        # print(i)
        #用户的个性签名提取
        description = re.findall('"description":".*?"',i)
        #r如果没有个性标签 就没有 就赋值为0
        if len(description) == 0:
            description = "无"
        else:
            description = description

        #标题截取
        user_name = str(re.findall('"name":".*?"',i)[0]).split(":")[1]
        #内容截取
        content = re.findall('"content":".*?"',a)
        #内容正则处理
        str_chuli = ''.join(content).split(":")[1]
        #用户主页链接提取
        userurl = c.split(":")[1].split('"')[1]
        userurl = "https://www.zhihu.com/people/"+userurl

        #汇总用户信息为字典 并返回
        item_userinfo.append({"description": description, "user_name": user_name, "content": str_chuli,"userinfo_url":userurl})
        # print(user_name)
    return item_userinfo

#用户详情页爬取函数
def home_crwal_request(home_url):

    headers = {
        'cookie': 'SESSIONID=1onFk2N4GxONhGQucLTF1gHjxautyXP67KoU5r9jSLV; osd=W10XB0mVHD0UQyBEP5ibKYfsf8Em0VROSjh4EHT3Iw5ac2ktDfUMbXlEK0U_R9o1Rhlsr4EnxEP7ozse6S6fKPQ=; JOID=Vl8QAEmYHjoTQy1GOJ-bJIXreMEr01NJSjV6F3P3LgxddGkgD_ILbXRGLEI_StgyQRlhrYYgxE75pDwe5CyYL_Q=; _zap=c7682c52-360f-44a9-95ed-e6b787d40a11; d_c0="ADAd15xBgRKPTl3ZxfbQPbd4LbpSdxtQ1f8=|1610691888"; _xsrf=9584bb49-f417-40c0-83d0-e40b557b9b9a; captcha_session_v2="2|1:0|10:1615786158|18:captcha_session_v2|88:R1Z3WHp0TWIvRU1jOW5aK0N5bVZOYkVra2FCZUt1eWhRZzJUWWF1b1p2Ukt0bUhRTGRHKzFBejRnc1M1N1pMSg==|5f31282d696351ea5c7bcdac15e25e894c518e0e6d0c41da98d933d4bd817dad"; captcha_ticket_v2="2|1:0|10:1615786169|17:captcha_ticket_v2|228:eyJhcHBpZCI6IjIwMTIwMzEzMTQiLCJyZXQiOjAsInRpY2tldCI6InQwMzdmV2JsWW9PNDJBYWIyNGhZdm80V05TYUFGbjBXaFhhS2QyMGRtVDUxeG9Ic3pJT2xWaFh3VFZHMXo0UVFsN25Tek1aRXJib0hzRGYyN0dwcm4yNnBHc2FxbzRvMjNMZTZoWUdqeEYzQnBnKiIsInJhbmRzdHIiOiJAWmFIIn0=|1e0242f221a3e48eb237077e18a0dad1c850a951221ec7d0d50dc702feb51de7"; tst=h; tshl=; q_c1=518151e75d3b493c975cc84440150ffd|1615786198000|1615786198000; SESSIONID=HOuXpTSMi34H6Be5Smw3AnckD67BWs9orHKeNyDSu6q; JOID=VFgTCksDkWj0OopTIQwRd2-T19EzTdgSpUzYBmNspFqzBc4wEgCVPps6ilonHGm1lNgFLav83xXWgLvkdqNbvg0=; osd=VFwUBU0DlW_7PIpXJgMXd2uU2NczSd8do0zcAWxqpF60CsgwFgeaOJs-jVUhHG2ym94FKazz2RXSh7TidqdcsQs=; l_n_c=1; n_c=1; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1615517406,1615786121,1615858909,1615884629; r_cap_id="NmE0NjNhYTA0ZmQ5NDczNWI5NjY1OGMwMDdhNDIzYjg=|1615947684|e1a182240128a1a1ddbbd335d078eee69b574276"; cap_id="NDk2MzNlNDJjMDg0NDAzMmJhNjcyMmI4YjBiNGFkNDY=|1615947684|82b4af491b95d5a12620b2d9a8a04e14db541efd"; l_cap_id="M2I2YmE2ODcxMGUxNDEzNWJiNGE2ZDVjYWVjZTExNzk=|1615947684|ce768d645626e54a3efec7b20e2667fc15fe215d"; atoken=43_i2T0BLV3XnlRvYLt4H_rlhQ_W_bQ-JetGBYDm8_wFp4gEKq7rDyqsFIy9lhd2L_OdyRpaI-qesejIVKkwLcG8hpSLDfDWsvA3o8IE8OD_tg; atoken_expired_in=7200; client_id="bzNwMi1qdTYtZm1qdGk5dURzQS1NVWs5WHNBVQ==|1615947693|3128d54e0737fc22342cb39326c5a393e0bcc1c6"; capsion_ticket="2|1:0|10:1615947693|14:capsion_ticket|44:OWFiZDI4MzZkZDQ3NDE4ZThiODE1M2ZjNDVhMDQxNGY=|f420e7cf3e349c6e23fdcc33850e38c69ed684abb4c6998f50ecebe1d2c53bbe"; z_c0="2|1:0|10:1615947713|4:z_c0|92:Mi4xRDNYaEdBQUFBQUFBTUIzWG5FR0JFaVlBQUFCZ0FsVk53TFUtWVFCRTI4MnVaRWVmYnpSZkp6RVlzUjVCM2ZsaGJ3|6e4dfb84ce68be84d71466624e755993d238e35cceb6596167907ec943f60743"; unlock_ticket="AFCXk8-qwhAmAAAAYAJVTchuUWCbVw4JQw7PCrawY__5XvczsbafzA=="; Hm_lpvt_98beee57fd2ef70ccdd5ca52b9740c49=1615947675; KLBRSID=4843ceb2c0de43091e0ff7c22eadca8c|1615947715|1615945435',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'etag': '5a3914c6-16bb3',
        'expires': 'Tue, 09 Mar 2021 03:46:28 GMT',
        'server': 'Tengine',
        'vary': 'Accept-Encoding'
    }
    try:
        response = requests.get(url=home_url, headers=headers)

        wb_data = response.text

        html = etree.HTML(wb_data.encode('utf-8'))

        data = re.findall('关注了</div><strong class="NumberBoard-itemValue" title=".*?"',wb_data)

        #关注了
        attention = ''.join(data).split('title=')[1].split('"')[1]

        data1 = re.findall('关注者</div><strong class="NumberBoard-itemValue" title=".*?"',wb_data)

        # 关注者
        follower = ''.join(data1).split('title=')[1].split('"')[1]

        #个性签名
        personalized_signature = ''.join(re.findall('<meta data-react-helmet="true" name="description" property="og:description" content=".*? 回答数 .*?，获得 .*? 次赞同"/>',wb_data)).split("content=")[1].split("回答数")[0]

        #回答数
        answer = ''.join(re.findall('<meta data-react-helmet="true" name="description" property="og:description" content=".*? 回答数 .*?，获得 .*? 次赞同"/>',wb_data)).split("content=")[1].split("回答数")[1].split("获得")[0].split("，")[0].split(" ")[1]

        #赞同次数
        endorse = ''.join(re.findall('<meta data-react-helmet="true" name="description" property="og:description" content=".*? 回答数 .*?，获得 .*? 次赞同"/>',wb_data)).split("content=")[1].split("回答数")[1].split("获得")[1].split("次赞同")[0].split(" ")[1]

        #个人简介
        individual_resume = ''.join(re.findall('isActive":.*?,"description":".*?"', wb_data)).split('"description":')[1]

        #所属行业
        industry_involved = ''.join(re.findall('"business":{"id":".*?","type":"topic","url":".*?","name":".*?","avatarUrl"', wb_data)).split("name")[1].split('":"')[1].split('"')[0]
        user_information_dict = {"attention":attention,"follower":follower,"personalized_signature":personalized_signature,"answer":answer,"endorse":endorse,"individual_resume":individual_resume,"industry_involved":industry_involved}
        # print(user_information_dict)
        return user_information_dict
    except:
        pass

#数据入库
def Full_data_entry_main():
    #知乎热榜的url
    zhihu_crawl_list = zhihu_crawl(url='https://www.zhihu.com/hot')

    dict_list_aa = []
    for i in zhihu_crawl_list:
        article_content_crawl_list = article_content_crawl(content_url=i["hot_title_url"])
        for j in article_content_crawl_list:
            i.update(j)
            home_crwal_request_dict = [home_crwal_request(home_url=j["userinfo_url"])]
            for aj in home_crwal_request_dict:
                i.update(aj)
        dict_list_aa.append(i)
    data_result = pd.DataFrame(dict_list_aa)
    print(data_result)
    pd.io.sql.to_sql(data_result, 'zhihu_data', mysql_engine(), schema='user_huaxiang', if_exists='append', index=False)

if __name__ == '__main__':
    #总函数调用
    Full_data_entry_main()

    # home_crwal_request(home_url='https://www.zhihu.com/people/nogirlnotalk')
    # article_content_crawl(content_url='https://www.zhihu.com/people/li-jing-51-64-97')
    # article_content_crawl(content_url="https://www.zhihu.com/question/449514194")
    # zhihu_crawl(url='https://www.zhihu.com/hot')