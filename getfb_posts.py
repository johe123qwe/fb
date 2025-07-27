import re
import time
import random
import json
import group_info_config

import requests

"""
20240418 增加获取抖音的数据
20241110 修复获取专页的数据
20241204 修复专页被封导致程序中止
20241209 修复专页不存在时的报错
"""

cookies = group_info_config.COOKIES


# 设置请求头
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "sec-ch-ua": '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
    "viewport-width": "1500",
    "sec-fetch-site": "same-origin",
    "sec-fetch-dest": "document",
    "sec-ch-ua-platform": "macOS",
    "sec-ch-prefers-color-scheme": "light",
    "upgrade-insecure-requests": "1",
    "cache-control": "max-age=0",
    "sec-ch-ua-mobile": "?0",
}

# 添加代理服务器 https://spys.one/free-proxy-list/DE/
proxies = {
    "http": "http://149.202.172.113:80",
    "http": "http://144.76.75.25:4444",
}

def request(url):
    try:
        print(41, len(cookies.keys()), url)
        cookie = random.sample(list(cookies.keys()), 1)[0]
        headers["Cookie"] = cookie
        print(cookies[headers['Cookie']], cookie)
        result = requests.get(url, headers=headers, timeout=10)
        while result is not None:
            return result, cookie
        else:
            request(url)
    except requests.exceptions.ConnectionError:
        pass

def request_tk(url):
    try:

        result = requests.get(url, headers={'accept': 'application/json'}, timeout=10)
        # print(result.json(), 78)
        while result is not None:
            return result, 0
        else:
            request_tk(url)
    except requests.exceptions.ConnectionError:
        pass

def get_fb_posts_local(group_id, max_retries=5, retry_count=0):
    """从本地获取 facebook 小组信息"""
    if retry_count >= max_retries:
        print(f"达到最大重试次数 {max_retries}，放弃获取 {group_id}")
        return "max_retries_reached", "error"
    
    try:
        response_result = request(
            f"https://www.facebook.com/groups/{group_id}/about"
        )
        
        if response_result is None:
            print(f"再次获取 {group_id} 数据")
            time.sleep(5)
            return get_fb_posts_local(group_id, max_retries, retry_count + 1)
            
        response, cookie_index = response_result
        
        if response.status_code != 200:
            print(f"状态码错误: {response.status_code}, 重试获取 {group_id}")
            time.sleep(5)
            return get_fb_posts_local(group_id, max_retries, retry_count + 1)
        
        # with open("fb_posts.txt", "w", encoding="utf-8") as f:
        #     f.write(f"{group_id} {response.status_code}\n")
        #     f.write(response.text)

        if r'\u5185\u5bb9\u6682\u65f6\u65e0\u6cd5\u663e\u793a' in response.text \
            or r'\u76ee\u524d\u7121\u6cd5\u67e5\u770b\u6b64' in response.text:
            print(f"内容暂时无法显示: {group_id}")
            return 'iderror', 'iderror'

        result_last_day = re.search(
            r'"number_of_posts_in_last_day":(.*?),', response.text
        ) 
        result_members = re.search(
            r'"group_total_members_info_text":"(?:.*?)(\d+(?:,\d+)*)\s*(?:\\u4eba|\\u4f4d\\u6210\\u54e1)',
            response.text
        )
        print(97, group_id, result_last_day, result_members)
        if result_last_day and result_members:
            try:
                return result_last_day.group(1), result_members.group(1)
            except Exception as e:
                print(f"提取结果异常: {e}, 重试获取 {group_id}")
                time.sleep(5)
                return get_fb_posts_local(group_id, max_retries, retry_count + 1)
        else:
            print(f"无法匹配结果模式: {group_id}")
            global cookies
            bad_fb = cookies[cookie_index]
            if len(cookies) > 1:
                cookies.pop(cookie_index)
                return "toomany", bad_fb
            else:
                return "error", "error"
                
    except Exception as e:
        print(f"处理过程异常: {e}, 重试获取 {group_id}")
        time.sleep(5)
        return get_fb_posts_local(group_id, max_retries, retry_count + 1)

def get_fbpage(page_id):
    """从本地获取 facebook 专页的信息
    有两种response:
    1. 在名字下面tabindex="0">([0-9]+)<!-- --> 个赞</a>
    2. 在简介里follower_count"""
    response_result = request("https://www.facebook.com/{}".format(page_id))
    response = response_result[0]
    while response.status_code == 200:
        try:
            result_dianzan = re.search(
                r'"text":"([0-9,.]+(?:\\u00a0\\u4e07)?)\s?\\u6b21\\u8d5e"\}', response.text
            )
            if not result_dianzan:
                raise ValueError("无法匹配点赞数的正则表达式结果")
            result_members = re.search(
                r'"text":"([0-9,.]+(?:\\u00a0\\u4e07)?)\s?\\u4f4d\\u7c89\\u4e1d"\}', response.text
            )
            if not result_members:
                raise ValueError("无法匹配粉丝数的正则表达式结果")
            dianzan = result_dianzan.group(1).replace(r"\u00a0", "").replace(r"\u4e07", "万")
            members = result_members.group(1).replace(r"\u00a0", "").replace(r"\u4e07", "万")
            if '万' in dianzan:
                dianzan = int(float(dianzan[:-1]) * 10000)
            if '万' in members:
                members = int(float(members[:-1]) * 10000)
            print(dianzan, members)
            return dianzan, members
        
        except ValueError as ve:
            print(f"值错误: {ve}", 152)
            return "error", "error"
        
        except Exception as e:
            print(f"未知错误: {e}", 159)
            bad_fb = cookies[response_result[1]]
            if len(cookies) > 1:
                cookies.pop(response_result[1])
                return "toomany", bad_fb
            else:
                return "error", "error"
    else:
        time.sleep(5)
        get_fbpage(page_id)


def get_tiktok(id):
    """获取tiktok数据
    stats":{"followerCount好友数":2667,"followingCount关注数":7604
    token: 0JgZVFZgnX055TlQjTa8yEJu8BoBzYycgJBD/Cv3tkp64mtZWvsq3QK0DA==
    """
    print(id, 146)
    response_result = request_tk("https://api.douyin.wtf/api/tiktok/web/fetch_user_profile?uniqueId={}".format(id.strip()))
    response = response_result[0]

    while response.status_code == 200:
        try:
            result_follower_count = re.search(
                r'"followerCount":([0-9]+)', response.text
            )  # 粉丝
            result_following_count = re.search(
                r'"followingCount":([0-9]+)', response.text
            )  # 关注数
            result_friend_count = re.search(
                r'"friendCount":([0-9]+)', response.text
            )  # 好友数
            result_heart_count = re.search(
                r'"heartCount":([0-9]+)', response.text
            )  # 点赞数

            follower_count = result_follower_count.group(1)
            following_count = result_following_count.group(1)
            friend_count = result_friend_count.group(1)
            heart_count = result_heart_count.group(1)

            print(
                "粉丝",
                follower_count,
                "关注",
                following_count,
                "好友",
                friend_count,
                "点赞",
                heart_count,
            )

            return follower_count, following_count, friend_count, heart_count

        except Exception as e:
            print(e, 159)
            return None, None, None, None


if __name__ == "__main__":
    # pass
    # get_fbpage("Orthodox001")
    # get_fb_posts_local("415269162732013")
    get_tiktok("roselynegrace")
