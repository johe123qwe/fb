import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
import random
from datetime import datetime
import click
import re
import group_info
import requests
import group_info_config


# Skype 账号、密码、小组ID
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
    19:f7a3617cf25a4558bb4eexxxxxx@thread.skype"""
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
                send_teams(id, skinfo)
    return ten_group[1]



def get_data(sheet_id, platform, send_type):
    """获取表格数据，返回一个二维数组，并发送"""
    result = group_info.get_sheets(sheet_id)
    df = result[0]
    num_rows, num_cols = df.shape
    name_id = (
        {}
    )
    for i in range(num_rows):
        id_list = []
        for j in range(1, num_cols):
            value = df.iloc[i, j]
            if value is not None:
                id_list.append(value)
                name_id[df.iloc[i, 0]] = id_list

    all_data = []
    for sheet in result[0][0]:
        result = send_info(sheet, sheet, name_id.get(sheet), sheet_id, platform, send_type)
        all_data.extend(result)
    return all_data

@click.command()
@click.option("-s", "--groupid", help="更新的表格 ID")
@click.option("-p", "--platform", help="平台名称")
@click.option("-S", "--sendtype", default="fb", help="发送的类型")
def main(groupid, platform, sendtype):
    get_data(groupid, platform, sendtype)


if __name__ == "__main__":
    main()
