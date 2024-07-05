import requests
import json


class LarkBotMsg:
    def __init__(self, url):
        self.url = url

    def txt_msg(self, txt, at_all=False):
        """ 发送文本消息 """
        headers = {'Content-Type': 'application/json'}
        text = txt
        if at_all is True:
            text = txt + '\n<at user_id="all">所有人</at>'
        data = {
            "msg_type": "text",
            "content": {
                "text": text
            }
        }
        data = json.dumps(data)
        res = requests.post(url=self.url, data=data, headers=headers)
        res_text = res.text
        print(res_text)

    def md_msg(self, title, md, at_all=False):
        headers = {'Content-Type': 'application/json'}
        md_txt = md
        if at_all is True:
            md_txt = md + '\n<at id=all></at>'
        data = {
            "msg_type": "interactive",
            "card": {
                "elements": [{
                    "tag": "div",
                    "text": {
                        "content": md_txt,
                        "tag": "lark_md"
                    }
                }],
                "header": {
                    "title": {
                        "content": title,
                        "tag": "plain_text"
                    }
                }
            },
            'type': 'event_callback',
            'token': 'PGcNCUNHaHueC1sS5A2oKhOFAQJvkMwB',
            'event': {
                'type': 'message',
                'msg_type': 'text',
                'open_id': 'tqz no open_id',
                'text': ''
            }
        }
        data = json.dumps(data)
        res = requests.post(url=self.url, data=data, headers=headers)
        res_text = res.text
        print(res_text)


if __name__ == '__main__':
    # 用自己的群做测试
    _url = 'https://open.larksuite.com/open-apis/bot/v2/hook/78cc36a3-1081-4f66-894f-6075e185c96f'

    _title = "lark bot测试"
    _content = "测试一下中文是否有乱码"

    LarkBotMsg(_url).md_msg(_title, _content, at_all=False)  # 发送markdown消息
