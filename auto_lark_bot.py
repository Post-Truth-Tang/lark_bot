import sys
sys.path.append('E:\\quant')
from common_func import *

import pandas as pd
import time as tm
import math

import requests

import warnings
warnings.filterwarnings('ignore')

# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

from lark_bot_msg import LarkBotMsg


class AutoLarkBot:

    urls = [
        # "https://open.larksuite.com/open-apis/bot/v2/hook/e296aa7b-088a-46d3-b1f0-caf4d6d8e6e0",  # 老板和Richard群的webhook-url
        # "https://open.larksuite.com/open-apis/bot/v2/hook/577fee8e-4a4d-48fd-aec7-383195e31bf5",  # Richard和自己群的webhook-url
        "https://open.larksuite.com/open-apis/bot/v2/hook/78cc36a3-1081-4f66-894f-6075e185c96f",  # 自己做测试的群
        # "https://open.larksuite.com/open-apis/bot/v2/hook/60b555f7-de4d-494c-b9f0-12ac484f4586",  # 覃老师在的测试群
    ]

    # local | server
    # week_rpt_content_ciphertext_analysis_link = "http://localhost/www_rsh_newest/index.php?s=/addon/Tracker/LarkBot/decode_week_rpt_content"
    # action_report_ciphertext_analysis_link = "http://localhost/www_rsh_newest/index.php?s=/addon/Tracker/LarkBot/decode_action_report_content"
    week_rpt_content_ciphertext_analysis_link = "https://121.37.140.88/www_rsh/index.php?s=/addon/Tracker/LarkBot/decode_week_rpt_content"
    action_report_ciphertext_analysis_link = "https://121.37.140.88/www_rsh/index.php?s=/addon/Tracker/LarkBot/decode_action_report_content"

    status_map = {
        '0': '等待触发',
        '1': '已触发',
        '2': '已手动终止',
        '3': '系统止盈',
        '4': '系统止损',
        '5': '等待终止结算'
    }

    action_type_map = {
        '1': '交易',
        '3': '调研',
        '5': '估值',
        '7': '其他',
    }

    scan_interval_secs: int = 60

    @classmethod
    def run(cls):
        """
        Add all change notify in one process.
        """

        last_time_map = cls.__get_last_time_map()
        while True:
            stock_recommendation_last_time_format = last_time_map['stock_recommendation'].strftime("%Y-%m-%d %H:%M:%S")
            modify_pos_last_time_format = last_time_map['modify_pos'].strftime("%Y-%m-%d %H:%M:%S")
            action_report_last_time_format = last_time_map['action_report'].strftime("%Y-%m-%d %H:%M:%S")
            week_rpt_last_time_format = last_time_map['week_rpt'].strftime("%Y-%m-%d %H:%M:%S")

            stock_recommendation_ret_df = pd.read_sql(f'SELECT * FROM `wp_fin_author_recommend` WHERE `create_time` > "{stock_recommendation_last_time_format}" order by create_time desc;', WWW_RSH_STR)
            last_time_map = cls.scan_stock_recommendation(stock_recommendation_ret_df=stock_recommendation_ret_df, last_time_map=last_time_map)

            modify_pos_ret_df = pd.read_sql(f'SELECT * FROM `wp_fin_strg_trade_value` WHERE `create_time` > "{modify_pos_last_time_format}" AND `id_strg_indicator`=1008 order by create_time desc;', PMS_STR)
            last_time_map = cls.scan_modify_pos(modify_pos_ret_df=modify_pos_ret_df, last_time_map=last_time_map)

            action_report_ret_df = pd.read_sql(f'SELECT * FROM `wp_fin_ticker_action` WHERE `update_time` > "{action_report_last_time_format}" order by update_time desc;', WWW_RSH_STR)
            last_time_map = cls.scan_action_report(action_report_ret_df=action_report_ret_df, last_time_map=last_time_map)

            week_rpt_ret_df = pd.read_sql(f'SELECT * FROM `wp_fin_week_rpt` WHERE `update_time` > "{week_rpt_last_time_format}" order by update_time desc;', WWW_RSH_STR)
            last_time_map = cls.scan_week_report(week_rpt_ret_df=week_rpt_ret_df, last_time_map=last_time_map)

            if 0 == len(stock_recommendation_ret_df) and 0 == len(modify_pos_ret_df) and 0 == len(action_report_ret_df) and 0 == len(week_rpt_ret_df):
                print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} no new data, sleep {cls.scan_interval_secs}s...')

            tm.sleep(cls.scan_interval_secs)


    @classmethod
    def scan_stock_recommendation(cls, stock_recommendation_ret_df, last_time_map):
        if len(stock_recommendation_ret_df) > 0:
            for index, row in stock_recommendation_ret_df.iterrows():
                title = f'股票推荐更新: {row.get("author_name", default="未知研究员")} - {row.get("stock_name", default="未知股票")}'

                content = cls.__get_stock_author_recommend(row_data=row)

                content += '\n'
                [LarkBotMsg(url).md_msg(title, content) for url in cls.urls]

            last_time_map['stock_recommendation'] = stock_recommendation_ret_df.iloc[0]['create_time']

        return last_time_map

    @classmethod
    def scan_modify_pos(cls, modify_pos_ret_df, last_time_map):
        if len(modify_pos_ret_df) > 0:
            for index, row in modify_pos_ret_df.iterrows():
                sql = f'SELECT value,create_time FROM `wp_fin_strg_trade_value` WHERE `id_strg_indicator`=1009 AND `id_strg`={row["id_strg"]} AND `id_ticker`={row["id_ticker"]} AND `id_position`={row["id_position"]}  AND `create_time`>="{last_time_map["modify_pos"]}" order by `create_time` desc limit 1;'
                ret_comment_df = pd.read_sql(sql, PMS_STR)

                ret_id_strg_df = pd.read_sql(f'SELECT id_author,strg_name FROM `wp_fin_strg` where `id`={row["id_strg"]}', PMS_STR)

                id_author = ret_id_strg_df.iloc[0]['id_author']
                ret_id_author_df = pd.read_sql(f'SELECT name,id_broker FROM `wp_fin_author` where `id`={id_author}', PMS_STR)
                ret_institute_df = pd.read_sql(f'SELECT name FROM `wp_fin_institute` where `id`={ret_id_author_df.iloc[0]["id_broker"]}', PMS_STR)
                ret_id_ticker_df = pd.read_sql(f'SELECT fullname FROM `wp_fin_ticker` where `id`={row["id_ticker"]}', PMS_STR)

                title = f'调仓通知: {ret_id_author_df.iloc[0]["name"]} - {ret_id_ticker_df.iloc[0]["fullname"]} - {row["value"]}'
                content = ''
                if 1 == len(ret_comment_df):
                    content += f'策略名称: {ret_id_strg_df.iloc[0]["strg_name"]}'
                    content += '\n'
                    content += f'经纪商: {ret_institute_df.iloc[0]["name"]}'
                    content += '\n\n'
                    content += f'调仓说明: {ret_comment_df.iloc[0]["value"]}'
                    content += '\n'

                if '' == content:  # 调仓类型和调仓说明未同步 (这是异常情况)
                    [LarkBotMsg(url).md_msg(title, '(有调仓变动, 但调仓类型和调仓说明未同步)') for url in cls.urls]
                else:
                    [LarkBotMsg(url).md_msg(title, content) for url in cls.urls]

            last_time_map['modify_pos'] = modify_pos_ret_df.iloc[0]['create_time']

        return last_time_map

    @classmethod
    def scan_action_report(cls, action_report_ret_df, last_time_map):
        if len(action_report_ret_df) > 0:
            for index, row in action_report_ret_df.iterrows():
                id_author_name = "未知研究员"
                wp_fin_author_df = pd.read_sql(f'SELECT * FROM `wp_fin_author` WHERE `id`="{row["id_author"]}";', WWW_RSH_STR)
                if 1 == len(wp_fin_author_df):
                    id_author_name = wp_fin_author_df.iloc[0]["name"]

                id_ticker_name = "未知股票"
                wp_fin_ticker_df = pd.read_sql(f'SELECT * FROM `wp_fin_ticker` WHERE `id`="{row["id_ticker"]}";', WWW_RSH_STR)
                if 1 == len(wp_fin_ticker_df):
                    id_ticker_name = wp_fin_ticker_df.iloc[0]["fullname"]

                [LarkBotMsg(url).md_msg(f"调研和研究: {id_ticker_name} - {id_author_name}", cls.__get_action_report_content(row_data=row)) for url in cls.urls]

            last_time_map['action_report'] = action_report_ret_df.iloc[0]['update_time']

        return last_time_map

    @classmethod
    def scan_week_report(cls, week_rpt_ret_df, last_time_map):
        if len(week_rpt_ret_df) > 0:
            for index, row in week_rpt_ret_df.iterrows():
                response = requests.get(cls.week_rpt_content_ciphertext_analysis_link, params={"id": row["id"]}, verify=False)
                content = '(lark 请求服务器解析 周报密文内容 失败)'
                if response.status_code == 200:
                    content = response.text

                id_author_name = pd.read_sql(f'SELECT * FROM `wp_fin_author` WHERE `id`="{row["id_author"]}";', WWW_RSH_STR).iloc[0]["name"]
                biz_sector_name = pd.read_sql(f'SELECT * FROM `wp_fin_biz_sector` WHERE `id`="{row["id_biz_sector"]}";', WWW_RSH_STR).iloc[0]["name"]

                title = f'周报更新: {id_author_name} - {row["rpt_date"]}'
                lark_content = f'板块: {biz_sector_name}\n\n内容: {content}'

                [LarkBotMsg(url).md_msg(title, lark_content) for url in cls.urls]

            last_time_map['week_rpt'] = week_rpt_ret_df.iloc[0]['update_time']

        return last_time_map

    @classmethod
    def __get_last_time_map(cls):
        last_time_map = {}

        stock_recommendation_df = pd.read_sql(f'SELECT * FROM `wp_fin_author_recommend` order by create_time desc limit 1;', WWW_RSH_STR)
        if 0 == len(stock_recommendation_df):
            last_time_map['stock_recommendation'] = pd.to_datetime('1970-01-01 00:00:00')
        else:
            last_time_map['stock_recommendation'] = stock_recommendation_df.iloc[0]['create_time']

        modify_pos_df = pd.read_sql(f'SELECT * FROM `wp_fin_strg_trade_value` where `id_strg_indicator`=1008 order by `create_time` desc limit 1;', PMS_STR)
        if 0 == len(modify_pos_df):
            last_time_map['modify_pos'] = pd.to_datetime('1970-01-01 00:00:00')
        else:
            last_time_map['modify_pos'] = modify_pos_df.iloc[0]['create_time']

        action_report_df = pd.read_sql(f'SELECT * FROM `wp_fin_ticker_action` order by update_time desc limit 1;', WWW_RSH_STR)
        if 0 == len(action_report_df):
            last_time_map['action_report'] = pd.to_datetime('1970-01-01 00:00:00')
        else:
            last_time_map['action_report'] = action_report_df.iloc[0]['update_time']

        week_rpt_df = pd.read_sql(f'SELECT * FROM `wp_fin_week_rpt` order by update_time desc limit 1;', WWW_RSH_STR)
        if 0 == len(week_rpt_df):
            last_time_map['week_rpt'] = pd.to_datetime('1970-01-01 00:00:00')
        else:
            last_time_map['week_rpt'] = week_rpt_df.iloc[0]['update_time']

        return last_time_map

    @classmethod
    def __get_stock_author_recommend(cls, row_data):
        content = f'类型: {row_data.get("position_type", default="未知类型")}单 ({cls.status_map.get(str(row_data["status"]))})'
        content += '\n'

        if row_data['order_price'] is None or math.isnan(row_data['order_price']):
            sql = f'SELECT wind_code FROM `wp_fin_ticker` WHERE `id`={row_data["id_ticker"]}'
            id_ticker_wind_code = pd.read_sql(sql, WWW_RSH_STR).iloc[0]['wind_code']

            if id_ticker_wind_code is None:
                content += f'当前市价: (wind 无相关数据)'
            else:
                from WindPy import w
                w.start()
                content += f'当前市价: {w.wsq(id_ticker_wind_code, "rt_last").Data[0][0]} (数据来自wind终端)'
                w.stop()

        else:
            content += f'买入价: {row_data["order_price"]}'

        content += '\n'
        content += f'目标价: {row_data["target_price"]}'

        content += '\n'
        if pd.isnull(row_data["rec_days"]):
            content += f'总天数: 0 天'
        else:
            content += f'总天数: {str(int(row_data["rec_days"]))} 天'

        content += '\n\n'
        content += f'推荐说明: {row_data["start_reason"]}'

        if row_data["end_reason"] is not None:
            content += '\n\n'
            content += f'终止说明: {row_data["end_reason"]}'

        return content

    @classmethod
    def __get_action_report_content(cls, row_data):
        content = ''
        content += '\n\n'
        content += f'活动类型: {cls.action_type_map.get(str(row_data.get("action_type", default="未知活动类型")))} \n\n'
        content += f'活动时间: {row_data.get("action_time", default="1970-01-01 00:00:00")} \n\n'

        response = requests.get(cls.action_report_ciphertext_analysis_link, params={"id": row_data['id']}, verify=False)
        data = {
            'title': "(lark 请求服务器解析 活动报告 标题 密文失败)",
            'content': "(lark 请求服务器解析 活动报告 内容 密文失败)",
            'attachments': "(lark 请求服务器解析 活动报告 附件 密文失败)"
        }
        if response.status_code == 200:
            data = response.json()

        content += f'活动标题: {data["title"]} \n\n'
        content += f'活动内容: {data["content"]} \n\n\n'

        related_industries = '无'
        if len(row_data["id_related_industries"]) > 0:
            related_industries = ', '.join(map(str, pd.read_sql(
                f'SELECT `name` FROM `wp_fin_ticker_action_related_industries` WHERE `id_related_industry` IN ({row_data["id_related_industries"]});',
                WWW_RSH_STR
            )['name'].values.tolist()))
        content += ('相关行业: ' + related_industries)

        content += '\n\n'
        row_data['attachments'] = data["attachments"]
        if row_data['attachments'] == '[]':
            content += f'附件内容: 无附件 \n'
        else:
            content += f'附件内容: {str(eval(row_data["attachments"]))[1:-1]} \n'

        return content


if __name__ == '__main__':
    print(f'lark bot is running.')
    AutoLarkBot.run()
