使用方法 

1. 安装虚拟环境
python -m venv .venv
pip install -r requirements.txt

2. group_info_config-example.py 改为 group_info_config.py 填写相关信息，fb 界面语言为简体中文，获取fb cookie

4. 到云端生成一个密钥文件, 命名为 token.json

5. 复制表格模板，开启所有人可以编辑权限
https://docs.google.com/spreadsheets/d/1s68ezBk9ZX1FjTYBOfJm6jNTKXXXXXX/edit?usp=sharing

6. 运行脚本获取数据
python group_info.py -p fb -s YOUR_SHEET_ID -t "Europe/Berlin"

发送消息
python send_group_info.py -s YOUR_SHEET_ID -p fb
备注：发送消息不再好用

对于 linux 系统可添加定时任务 /etc/crontab
```bash
5 */4 * * * root cd /home/admin_tools/ && mvenv/bin/python3.11 group_info.py -p fb -s YOUR_SHEET_ID -t "Europe/Berlin"
3 23 * * * root cd /home/admin_tools/ && mvenv/bin/python3.11 send_group_info.py -s YOUR_SHEET_ID -p fb
```