import re
import redis
import warnings
import requests
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')

#配置redis 实现分布式

# redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
# redis_conn = redis.Redis(connection_pool=redis_pool)


def mysql_engine():
    user ='root'
    passwd ='123456'
    host ='10.228.83.123'
    port ='13306'
    dbname1 = 'user_huaxiang'
    engine2 = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"%(user,passwd,host,port,dbname1))
    return engine2

def request_url_title(url):
    headers = {
        'cookie': 'SINAGLOBAL=5805082080385.267.1614156573992; UOR=,,www.baidu.com; login_sid_t=832db90dd4cb1e7ac92cb798b41ea05a; cross_origin_proto=SSL; _s_tentry=www.baidu.com; Apache=4267083072776.9624.1615513358440; ULV=1615513358445:4:3:3:4267083072776.9624.1615513358440:1615283575094; wb_view_log=1920*10801; ALF=1647049431; wvr=6; wb_view_log_7246103873=1920*10801; SSOLoginState=1615528220; SUB=_2A25NT3FMDeRhGeFM7FUX9C_PyT-IHXVusB8ErDV8PUJbkNAKLRXVkW1NQKYgDHFgVPqHVZG3sloLkhxLJxHWtduU; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WW3lxOrSkxKAl52O0wqeF0L5NHD95QNeoMNSoBpe0z0Ws4Dqcjbi--NiK.Xi-2Ri--ciKnRi-zNSKeXShn0SK-7S8Yc1hzpSntt; wb_view_log_7277641323=1920*10801; webim_unReadCount=%7B%22time%22%3A1615541741454%2C%22dm_pub_total%22%3A4%2C%22chat_group_client%22%3A0%2C%22chat_group_notice%22%3A0%2C%22allcountNum%22%3A42%2C%22msgbox%22%3A0%7D',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'etag': '5a3914c6-16bb3',
        'expires': 'Tue, 09 Mar 2021 03:46:28 GMT',
        'server': 'Tengine',
        'vary': 'Accept-Encoding'
    }
    # url = "https://s.weibo.com/top/summary"
    response = requests.get(url=url,headers = headers)
    wb_data = response.text.encode().decode('utf-8')
    html = etree.HTML(wb_data.encode('utf-8'))
    # print(wb_data)
    com_list = html.xpath('//table//tbody//tr')
    result_list = []
    for i in com_list:
        title = ''.join(i.xpath("./td//a//text()"))
        # result_list.append({"标题":title})
        href = ''.join(i.xpath("./td//a//@href"))
        if href=="javascript:void(0);":
            pass
        else:
            #拼接详情页url
            href_join_url = "https://s.weibo.com"+href
            result_list.append({"标题":title,"url":href_join_url})
    return result_list

def detail_request(detail_url):
    headers = {
        'cookie': 'SINAGLOBAL=5805082080385.267.1614156573992; UOR=,,www.baidu.com; login_sid_t=832db90dd4cb1e7ac92cb798b41ea05a; cross_origin_proto=SSL; _s_tentry=www.baidu.com; Apache=4267083072776.9624.1615513358440; ULV=1615513358445:4:3:3:4267083072776.9624.1615513358440:1615283575094; ALF=1647049431; wvr=6; SSOLoginState=1615528220; SUB=_2A25NT3FMDeRhGeFM7FUX9C_PyT-IHXVusB8ErDV8PUJbkNAKLRXVkW1NQKYgDHFgVPqHVZG3sloLkhxLJxHWtduU; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WW3lxOrSkxKAl52O0wqeF0L5NHD95QNeoMNSoBpe0z0Ws4Dqcjbi--NiK.Xi-2Ri--ciKnRi-zNSKeXShn0SK-7S8Yc1hzpSntt; webim_unReadCount=%7B%22time%22%3A1615788445961%2C%22dm_pub_total%22%3A4%2C%22chat_group_client%22%3A0%2C%22chat_group_notice%22%3A0%2C%22allcountNum%22%3A50%2C%22msgbox%22%3A0%7D',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'etag': '5a3914c6-16bb3',
        'expires': 'Tue, 09 Mar 2021 03:46:28 GMT',
        'server': 'Tengine',
        'vary': 'Accept-Encoding'
    }
    response = requests.get(url=detail_url,headers = headers)
    wb_data = response.text.encode().decode('utf-8')
    html = etree.HTML(wb_data.encode('utf-8'))
    detail = html.xpath('//*[@id="pl_feedlist_index"]/div//div//div')
    # detail = html.xpath('//*[@id="pl_feedlist_index"]//div//div//div//div//div')
    # print(detail)
    detail_result_list = []
    for i in detail:
        detail_title = i.xpath('./div[@class="card-feed"]//div[@class="content"]//div[@class="info"]//div//a//text()')
        if len(detail_title)==0:
            pass
        else:
            detail_title = detail_title[2]
        detail_content = ''.join(i.xpath('./div[@class="card-feed"]//div//p//a//text()'))
        if detail_content == '':
            pass
        else:
            detail_content = detail_content

        transmit = i.xpath("./div[@class='card-act']//ul//li[2]//a//text()")
        if len(transmit) == 0:
            pass
        else:
            transmit = ''.join(transmit).split("转发")[1]

        comment = i.xpath("./div[@class='card-act']//ul//li[3]//a//text()")
        if len(comment) == 0:
            pass
        else:
            comment = ''.join(transmit)

        give_a_like = i.xpath("./div[@class='card-act']//ul//li[4]//a//text()")
        # print(give_a_like)
        if len(give_a_like) == 0 or len(give_a_like) == 1:
            pass
        else:
            give_a_like = give_a_like[1]

        detail_url_url = i.xpath('./div[@class="card-feed"]//div[@class="content"]//div[@class="info"]//div//a//@href')
        #详情页微博用户主页url拼接
        if len(detail_url_url) == 0:
            pass
        else:
            detail_url_url = 'https:'+detail_url_url[2]
            detail_result_list.append({"detail_page_title":detail_title,"detail_page_content":detail_content,"transmit":transmit,"comment":comment,"give_a_like":give_a_like,"main_page_url":detail_url_url})
    return detail_result_list

#详情页主页 关注 粉丝 微博 xpath  //*[@id="Pl_Core_T8CustomTriColumn__3"]//div//div//div//table//tbody//tr//td[]//strong
def homepage_request(homepage_url):
    headers = {
        'cookie': 'SINAGLOBAL=5805082080385.267.1614156573992; UOR=,,www.baidu.com; login_sid_t=832db90dd4cb1e7ac92cb798b41ea05a; cross_origin_proto=SSL; _s_tentry=www.baidu.com; Apache=4267083072776.9624.1615513358440; ULV=1615513358445:4:3:3:4267083072776.9624.1615513358440:1615283575094; wb_view_log=1920*10801; ALF=1647049431; wvr=6; wb_view_log_7246103873=1920*10801; SSOLoginState=1615528220; SUB=_2A25NT3FMDeRhGeFM7FUX9C_PyT-IHXVusB8ErDV8PUJbkNAKLRXVkW1NQKYgDHFgVPqHVZG3sloLkhxLJxHWtduU; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WW3lxOrSkxKAl52O0wqeF0L5NHD95QNeoMNSoBpe0z0Ws4Dqcjbi--NiK.Xi-2Ri--ciKnRi-zNSKeXShn0SK-7S8Yc1hzpSntt; wb_view_log_7277641323=1920*10801; webim_unReadCount=%7B%22time%22%3A1615541741454%2C%22dm_pub_total%22%3A4%2C%22chat_group_client%22%3A0%2C%22chat_group_notice%22%3A0%2C%22allcountNum%22%3A42%2C%22msgbox%22%3A0%7D',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
        'etag': '5a3914c6-16bb3',
        'expires': 'Tue, 09 Mar 2021 03:46:28 GMT',
        'server': 'Tengine',
        'vary': 'Accept-Encoding'
    }
    response = requests.get(url=homepage_url,headers = headers)
    wb_data = response.text
    #关注数
    # attention_num = ''.join(re.findall('<strong class=.*?">.*?<.*?strong><span class=.*?".*?">关注<.*?span>', wb_data)).split('class=\\"W_f18\\">')[1].split('<\/strong>')[0]
    attention_num = ''.join(re.findall('<strong class=.*?">.*?<.*?strong><span class=.*?".*?">关注<.*?span>', wb_data)).split('>')[1].split('<\/strong')[0]
    # print(attention_num)
    #粉丝数
    # fans_num = ''.join(re.findall('<strong class=.*?".*?">.*?<.*?strong><span class=.*?".*?">粉丝<.*?span><.*?a><.*?td>',wb_data)).split('<strong class=\\"W_f18\\">')[2].split("<\/strong>")[0]
    fans_num = ''.join(re.findall('<strong class=.*?".*?">.*?<.*?strong><span class=.*?"S_txt2.*?">粉丝<.*?span>', wb_data)).split('<strong class=')[2].split('>')[1].split('<\/strong')[0]
    #博主类型
    blog_type = re.findall('<em title= .*?".*?" class=.*?"W_icon icon_pf_approve.*?" suda-uatrack=.*?"key=profile_head&value=vuser_guest.*?">',wb_data)[0].split('<em title= \\')[1].split("\\")[0].split('"')[1]
    #微博总数
    # weibo_num = ''.join(re.findall('<strong class=.*?"W_f18.*?">.*?<.*?strong><span class=.*?"S_txt2.*?">微博<.*?span><.*?a>', wb_data)).split('<strong class=\\"W_f18\\">')[3].split('<\/strong>')[0]
    weibo_num = ''.join(re.findall('<strong class=.*?".*?">.*?<.*?strong><span class=.*?"S_txt2.*?">微博<.*?span>', wb_data)).split('<strong class=')[3].split('>')[1].split('<\/strong')[0]
    # print(weibo_num)
    item_dict_result = {"attention_num":attention_num,"fans_num":fans_num,"blog_type":blog_type,"weibo_num":weibo_num}
    return item_dict_result



def crawl_data_write():
    data_set_name = 'weibo_crawl_topic'
    data = request_url_title(url="https://s.weibo.com/top/summary")

    dict_aa_list = []


    for i in data:
    #     msg_name = str(i["url"])
    #     # if redis_conn.sismember(data_set_name, msg_name):
    #     #     continue
    #     # else:
    #     #     redis_conn.sadd(data_set_name, msg_name)
        detail_request_list = detail_request(detail_url=i["url"])
        for j in detail_request_list:
            i.update(j)
            platform_cid = j["main_page_url"].split("/")[3].split("?")[0]
            a = 'https://weibo.com/u/{}?refer_flag=1001030103_&is_hot=1'.format(platform_cid)
            try:
                data_re = [homepage_request(homepage_url=a)]
                for aj in data_re:
                    i.update(aj)
            except:
                pass
        dict_aa_list.append(i)
    data_list_type = pd.DataFrame(dict_aa_list)
    print(data_list_type)
    pd.io.sql.to_sql(data_list_type, 'weibo_data', mysql_engine(), schema='user_huaxiang', if_exists='append', index=False)



if __name__ == '__main__':
    crawl_data_write()




