import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
import random
from datetime import datetime
import click
import re
import group_info
import requests
import group_info_config

# pip install facebook-scraper

'''
20240906 去掉一些不必要的表情符号
20240419 增加发送抖音的数据
20250122 加上一些过滤的表格符号
20250315 改为 Teams 发送消息
20250526 去掉 @
'''

# Skype 账号、密码、小组ID
sk_username = group_info_config.SK_USERNAME
sk_password = group_info_config.SK_PASSWORD
groupid = group_info_config.GROUP_ID

tday = datetime.now()  # 当前时间
tday_ = tday.strftime("%Y-%m-%d %H:%M:%S")  # 当前日期时间
tday__ = tday.strftime("%Y-%m-%d")  # 当前日期时间

emoji_list = [
    "☘️","🥕","🍀","🚗","🍊","🍋","🍍","🥭","🍎","🍓","🍒","🍆","🫒","🍅","🍔","🗓️","🟢","🔵","😊","😬","🥺","👀","🫑","🌸","🏵️","🌺","🌻","🚗","🚕","🚙","👍","🌟","☀️","🥦","🥬","🥒","🌽","🏆","🎁","💖","💝","🧡","✅","🌹","💞"
]

remove_emoji_list = [
    "🙏", "🙂", "💞", "💙", "☘️","🥕","🍀","🚗","🍊","🍋","🍍","🥭","🍎","🍓","🍒","🍆","🫒","🍅","🍔","🗓️","🟢","🔵","😊","😬","🥺","👀","🫑","🌸","🏵️","🌺","🌻","🚗","🚕","🚙","👍","🌟","☀️","🥦","🥬","🥒","🌽","🏆","🎁","💖","💝","🧡","✅","🌹","💗","🎀","📚","📖","❤️","🎼","🇵🇱","🎂","🙌","😇","✨","☕","♥️","🤗","🌼","🌷","🎵","🎶","❣",
    "🌾","🚕","🌱","❤","🥀","💐",
]

def remove_specific_emojis(text, emoji_list):
    # 构建正则表达式，匹配指定的表情符号
    emoji_pattern = re.compile("|".join(re.escape(emoji) for emoji in emoji_list))

    # 移除指定的表情符号
    return emoji_pattern.sub(r'', text)

def send_teams(id, message):
    """发送消息
    19:f7a3617cf25a4558bb4eefd53e05a2b4@thread.skype"""
    url = "https://teamsapiwvqtegdpe.1010822.xyz/api/send-by-convid"
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    print("发送消息内容：", message)
    data = {
        "message": message,
        "conversation_id": "{}".format(id, ),
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        print("响应内容：", response.text)
        return 1
    else:
        print("请求失败，状态码:", response.status_code)
        return 0

# def send_sk(info, SK_ID):
#     """发送 skype 信息,
#     rich=True 为富文本，可以@用户，但是开启rich后
#     使用的表情不能为skype自带的表情，可以使用windows或mac系统
#     自带的表情。"""
#     try:
#         sk = Skype(sk_username, sk_password)
#         ch = sk.chats[SK_ID]  # 给小组发信息
#         ch.sendMsg(info, rich=True)
#         return 1
#     except Exception as e:
#         return 0


def skype_info(title, li, sheet_id, num=4):
    """生成要发送的信息"""
    sheet_dic = group_info.get_id_name(sheet_id)
    content = """[{0}](https://docs.google.com/spreadsheets/d/{1}/edit#gid={2}) {3} 报告\n""".format(title, sheet_id,sheet_dic.get(title, None), tday__)
    for index in range(len(li)):
        result = "{}{} [{}](https://www.facebook.com/groups/{}) 新增 {}".format(
            random.sample(emoji_list, 1)[0],
            li[index][0],
            remove_specific_emojis(li[index][1].strip(), remove_emoji_list),
            li[index][2],
            li[index][num],
        )
        content += result + "\n"
    return content

def skype_info_tk(title, li, sheet_id, content):
    """生成要发送的信息"""
    sheet_dic = group_info.get_id_name(sheet_id)
    content = """[{0}](https://docs.google.com/spreadsheets/d/{1}/edit#gid={2}) {3} 新增{4}\n""".format(title, sheet_id,sheet_dic.get(title, None), tday__, content)
    for index in range(len(li)):
        result = "{}{} 新增 {}".format(
            random.sample(emoji_list, 1)[0],
            li[index][0],
            li[index][1]
        )
        content += result + "\n"
    return content


def skype_info2(li):
    """生成要发送的信息2"""
    content = "自建小组 {} 新增人数最少的小组\n".format(tday__)
    for index in range(len(li)):
        result = "{}{} [{}](https://www.facebook.com/groups/{}) 新增 {}".format(
            "🚀", li[index][0], li[index][1].strip(), li[index][2], li[index][4]
        )
        content += result + "\n"
    return content


def send_info(title, sheet_name, ids, sheet_id, platform, send_type):
    """发送信息1"""
    if platform == "fb":
        ten_group = group_info.group_sort(sheet_name, sheet_id)
        if ids is not None:
            skinfo = skype_info(title, ten_group[0], sheet_id, 4)
            for id in ids:
                send_teams(id, skinfo)
    elif platform == "tiktok":
        ten_group = group_info.group_sort_tk(sheet_name, sheet_id, send_type)
        if ids is not None:
            if send_type == str(8):
                content = "好友"
            else:
                content = "关注"
            skinfo = skype_info_tk(title, ten_group, sheet_id, content)
            for id in ids:
                # print(skinfo)
                send_teams(id, skinfo)
    return ten_group[1]


def dic_title():
    """title"""
    dic = {
        "机器人报数": "5000人以下小组",
        "机器人报数5000人以上": "5000人以上小组",
        "主攻组报数": "主攻组",
        "机器人报数10000人冲刺": "10000人冲刺",
        "宗派小组报数": "宗派小组报数",
    }
    return dic


def get_data(sheet_id, platform, send_type):
    """获取表格数据，返回一个二维数组，并发送"""
    result = group_info.get_sheets(sheet_id)
    df = result[0]
    num_rows, num_cols = df.shape
    name_id = (
        {}
    )  # {'外邦5000以上小组': ['19:7167c78188064c74bd9d5d9a5e782cd0@thread.skype', '123'], '外邦发展中小组': ['19:373648266d8848a2b85ed9f36a163cf7@thread.skype']}
    for i in range(num_rows):
        id_list = []
        for j in range(1, num_cols):
            value = df.iloc[i, j]
            if value is not None:
                id_list.append(value)
                name_id[df.iloc[i, 0]] = id_list

    # sheets = name_id.keys()
    all_data = []
    for sheet in result[0][0]:
        result = send_info(sheet, sheet, name_id.get(sheet), sheet_id, platform, send_type)
        all_data.extend(result)
    return all_data

@click.command()
@click.option("-s", "--groupid", help="更新的表格 ID")
@click.option("-p", "--platform", help="平台名称")
@click.option("-S", "--sendtype", default="7", help="发送的类型")
def main(groupid, platform, sendtype):
    data = get_data(groupid, platform, sendtype)
    """发送来人最少的小组
    sorted_list = sorted(data, key=lambda x: int(x[4]))
    nlist = sorted_list[0:5]
    for index in range(5, len(sorted_list)):
        if sorted_list[5][4] == sorted_list[index - 1][4]:
            nlist.append(sorted_list[index])
    nlist.reverse()
    skinfo = skype_info2(nlist) # 从所有数据中找到来人最少的小组
    send_sk(skinfo, groupid) 
    """


if __name__ == "__main__":
    main()
