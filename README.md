## 使用方法 

安装虚拟环境
python -m venv .venv
pip install -r requirements.txt

修改配置文件
```bash
cp group_info_config-example.py group_info_config.py 
```
填写相关信息，fb 界面语言为简体中文，获取 cookie

到[Google Cloud](https://console.cloud.google.com/) 生成一个密钥文件, 放在项目根目录，命名为 token.json

复制表格模板，开启所有人可以编辑权限或你的 token.json 中的邮箱帐号
https://docs.google.com/spreadsheets/d/1kjbzjVulFYb75b86smGrQZHG1naN1IzK7LW5e7rxtxk

运行脚本获取数据
```bash
python group_info.py -p fb -s YOUR_SHEET_ID -t "Europe/Berlin"
```

发送消息
```bash
python send_group_info.py -s YOUR_SHEET_ID -p fb
```
备注：发送消息不再好用

对于 linux 系统可添加定时任务 /etc/crontab
```bash
5 */4 * * * root cd /home/admin_tools/ && mvenv/bin/python3.11 group_info.py -p fb -s YOUR_SHEET_ID -t "Europe/Berlin"
3 23 * * * root cd /home/admin_tools/ && mvenv/bin/python3.11 send_group_info.py -s YOUR_SHEET_ID -p fb
```