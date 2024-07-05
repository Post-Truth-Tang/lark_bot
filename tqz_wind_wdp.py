# coding=utf-8
import datetime

import numpy as np
from function import *

import warnings
warnings.filterwarnings('ignore')

pd.set_option('mode.chained_assignment', None)  # 避免使用链式索引

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


ID_A_MARKET = 11
ID_HK_MARKET = 8
ID_BROKER_MAX = 185  # TODO 机构数量, 需具体在wind终端看看具体对应的是哪些机构;

wind_shszhk_cmd = {
    "foreignholdingchangebysecurity": 'w.wset("foreignholdingchangebysecurity","date=xxx;exlude=on;field=windcode,secname,date,totalinstitutioncount,totalshares,totalmarketvalue,totalsharesproportion,totaltradablesharesproportion,totalfreesharesproportion,northenshares,northeninstitutioncount,northenmarketvalue,northensharesproportion,northentradablesharesproportion,northenfreesharesproportion,northentop1institution,reportdate,qfiishares,qfiiinstitutioncount,qfiimarketvalue,qfiisharesproportion,qfiitradablesharesproportion,qfiifreesharesproportion,qfiitop1institution,roe,pe,pb,close,industry",usedf=True)',
    "shhkactivitystock": 'w.wset("shhkactivitystock","startdate=xxx;enddate=xxx;direction=southnorth;field=windcode,name,date,type,amount,buyamount,sellamount,buynetamount,close,currency,changepct,turnoverrate,pe,pb,cscrindustry,windindustry",usedf=True)',
    "hkstockholdings": 'w.wset("hkstockholdings","date=xxx;field=wind_code,sec_name,hold_stocks,holding_marketvalue,calculate_ratio",usedf=True)'
}

wind_shszhk_field_in_db_replace = {
    "foreignholdingchangebysecurity": {
        'windcode': 'wind_code',
        'secname': 'sec_name',
        'totalinstitutioncount': 'total_institution_count',
        'totalshares': 'total_shares',
        'totalmarketvalue': 'total_marketvalue',
        'totalsharesproportion': 'total_shares_proportion',
        'totaltradablesharesproportion': 'total_tradable_shares_proportion',
        'totalfreesharesproportion': 'total_freeshares_proportion',
        'northenshares': 'northen_shares',
        'northeninstitutioncount': 'northen_institution_count',
        'northenmarketvalue': 'northen_marketvalue',
        'northensharesproportion': 'northen_shares_proportion',
        'northentradablesharesproportion': 'northen_tradable_shares_proportion',
        'northenfreesharesproportion': 'northen_freeshares_proportion',
        'northentop1institution': 'northen_top1_institution',
        'reportdate': 'report_date',
        'qfiishares': 'qfii_shares',
        'qfiiinstitutioncount': 'qfii_institution_count',
        'qfiimarketvalue': 'qfii_marketvalue',
        'qfiisharesproportion': 'qfii_shares_proportion',
        'qfiitradablesharesproportion': 'qfii_tradable_shares_proportion',
        'qfiifreesharesproportion': 'qfii_freeshares_proportion',
        'qfiitop1institution': 'qfii_top1_institution',
        'close': 'day_close_price',
    },
    "shhkactivitystock": {
        'windcode': 'wind_code',
        'name': 'sec_name',
        'buyamount': 'buy_amount',
        'sellamount': 'sell_amount',
        'buynetamount': 'buy_net_amount',
        'close': 'day_close_price',
        'changepct': 'change_pct',
        'turnoverrate': 'turnover_rate',
        'cscrindustry': 'cscr_industry',
        'windindustry': 'wind_industry',
        'pb': 'pb(mrq)',
    },
    "est": {
        'est_estnewtime_inst': 'new_est_time',
        'est_newratingtime_inst': 'new_rating_time'
    },
}

table_replace = {
    # "foreignholdingchangebysecurity": "wp_fin_foreign_shares_a",  # 拉取的数据量有限, 所以测试时先注释了
    "shhkactivitystock": "wp_fin_top10_shhkactivitystock",
    "hkstockholdings": "wp_fin_hk_stock_connect_share"
}
unique_index = {
    "foreignholdingchangebysecurity": ["date"],
    "shhkactivitystock": ["date", "type"],
    "hkstockholdings": ["date"]
}
year = str(date.today().year + 1)
trade_date = datetime.now().strftime('%Y%m%d')

est_refresh_time_cmd = f'w.wss("WIND_CODE_LIST", "est_estnewtime_inst,est_newratingtime_inst","year={year};tradeDate={trade_date};broker=BROKER",usedf=True)'
est_rpt_details_cmd = f'w.wss("WIND_CODE_LIST", "est_rpttitle_inst,est_rptabstract_inst,est_netprofit_inst,est_sales_inst,est_eps_inst,est_orgrating_inst,est_scorerating_inst,est_stdrating_inst,est_highprice_inst,est_lowprice_inst,est_prehighprice_inst,est_prelowprice_inst,est_frstratingtime_inst,est_ratinganalyst,est_estanalyst,est_pctchange,est_newratingtime_inst,est_estnewtime_inst","tradeDate={trade_date};broker=BROKER;unit=1;year=YEAR",usedf=True)'


class DBTool:

    dbe, dbc, dbcur = list(connect_to_db(Const.www_rsh).values())
    dbcur, dbcur2, dbcur_w_cmd, dbcur_w_set_cmd = is_db_cursor_connected(connection_info=Const.www_rsh, dbc=dbc, cursor_count=4)

    @classmethod
    def get_last_date(cls, table_name: str, default_diff_days: int = 90) -> date:
        """
        get last date from appoint table.
        :param table_name: table name in db.
        :param default_diff_days: distance from today.
        :return: last date in db.table_name or distance from today days.
        """

        sql = f"select date from {table_name}"
        sql += f" order by date desc;"

        ret_df = pd.read_sql(sql, cls.dbc)

        last_date_str = datetime.now().strftime("%Y%m%d")
        if len(ret_df) != 0:
            last_date = ret_df.iloc[0]['date']
            if (datetime.now().date() - last_date.date()).days >= default_diff_days:
                last_date = datetime.strptime(last_date_str, '%Y%m%d') - timedelta(default_diff_days)

            last_date_str = str(last_date).split(' ')[0].replace('-', '')
        else:  # empty table.
            last_date = datetime.strptime(last_date_str, '%Y%m%d') - timedelta(default_diff_days)
            last_date_str = str(last_date).split(' ')[0].replace('-', '')

        return datetime.strptime(last_date_str, '%Y%m%d').date()


    @classmethod
    def manual_fetch_wind_data(cls, w_cmd):
        wind_field = w_cmd['wind_field']
        cmd = wind_shszhk_cmd[wind_field]

        cmd = cmd.replace("xxx", datetime.strptime(w_cmd['request_date'], "%Y%m%d").strftime("%Y-%m-%d"))

        w_result = eval(cmd)
        datas = w_result[1]
        if not datas.empty:  # 港股通不存在需要重置字段名
            if wind_field in wind_shszhk_field_in_db_replace.keys():
                field_name_dict = wind_shszhk_field_in_db_replace[wind_field]
                datas.rename(columns=field_name_dict, inplace=True)

            table = table_replace[wind_field]

            datas.set_index("wind_code", drop=False, inplace=True)
            w_cmd['return_value'] = datas.shape[0]
            date_list = ''
            date_list = [str(w_cmd['request_date'])]

            if 'date' in datas.columns.to_list():  # Wind的港股API中通没有date字段
                datas = datas[datas['date'] == pd.to_datetime(w_cmd['request_date'])]
            else:
                datas['date'] = pd.to_datetime(w_cmd['request_date'])

            date_filter = ",".join(map(str, date_list))

            # 循环选择具有unique的字段
            sql = f"select id, wind_code"
            for unique_field in unique_index[wind_field]:
                sql = sql + f" ,{unique_field}"
            sql = sql + f"  from {table} where date in ({date_filter})"

            exist_datas = pd.read_sql(sql, cls.dbc)

            duplicate_datas = pd.DataFrame()
            if not exist_datas.empty:
                # 区分索引列和数据列

                exist_datas.set_index("wind_code", drop=False, inplace=True)
                duplicate_datas = exist_datas.copy(deep=True)
                duplicate_datas.index.name = 'windcode'
                datas.index.name = 'windcode'
                # 循环选择具有uinque的字段有重复的行数据
                on_columns = unique_index[wind_field] + ['windcode']
                duplicate_datas = pd.merge(duplicate_datas, datas, how='inner', on=on_columns, suffixes=('', '_'))

            for index, row in duplicate_datas.iterrows():
                # target=duplicate_datas[duplicate_datas['wind_code'] == row['wind_code']]
                sql = f"delete from {table} where wind_code='{row['wind_code']}'"

                # 循环删除具有uinque的字段重复行数据
                for unique_field in unique_index[wind_field]:
                    # target=target[target[unique_field]==row[unique_field]]
                    sql = sql + f"  and {unique_field}='{row[unique_field]}'"
                # id=target['id'].iloc[0]
                # sql = f"-- delete from {table} where id={id}"

                cls.dbcur2.execute(sql)

            # tqz add.
            if wind_field == 'hkstockholdings':
                datas['hold_stocks'] = datas['hold_stocks'].astype(c_int64)
            elif wind_field == 'foreignholdingchangebysecurity':
                datas.dropna(axis=0, inplace=True)
                datas['total_shares'] = datas['total_shares'].astype(c_int64)
                datas['northen_shares'] = datas['northen_shares'].astype(c_int64)
            elif wind_field == 'shhkactivitystock':
                pass
            else:
                print(f'Bad wind_field: {wind_field}')
                # assert True, f'Bad wind_field: {wind_field}'

            # print("datas: " + str(datas[datas['wind_code'].isin(['2380.HK', '6878.HK', '0493.HK'])]))
            datas.to_sql(table, con=cls.dbe, if_exists="append", index=False)


    @classmethod
    def clear_cmd_tables(cls):
        status, cmd_tables = 1, ['wp_fin_blmb_period_cmd', 'wp_fin_wind_period_cmd']
        for cmd_table in cmd_tables:
            cls.dbcur.execute(f'delete from {cmd_table} where status={status};')



class DBDataManager:

    @classmethod
    def sync_data_to_db(cls, sync_type: str = 'common', clear_cmd_tables: bool = False):
        if clear_cmd_tables is True:
            DBTool.clear_cmd_tables()

        if not connect_to_wind():
            exit()

        if sync_type == 'common':
            cls.__sync_common_data()
        elif sync_type == 'wind_report':
            cls.__sync_wind_report_data()
        else:
            print(f'Bad sync_type: {sync_type}')
            # assert True, f'Bad sync_type: {sync_type}'


    @classmethod
    def __sync_wind_report_data(cls):
        sql = f'SELECT DISTINCT id_ticker,b.id_market,b.fullname as sec_name,b.wind_name as wind_code,b.blmb_name from wp_fin_ticker_pool_member a,wp_fin_ticker b where b.id_market IN ({ID_A_MARKET},{ID_HK_MARKET}) and a.id_ticker=b.id'
        track_stock = pd.read_sql_query(sql, DBTool.dbc)
        track_stock.loc[track_stock['wind_code'].isnull(), 'wind_code'] = track_stock['blmb_name'].apply(
            lambda x: x if 'HK' not in str.upper(x) else x[:str.upper(x).index(' HK')].rjust(4, '0') + ".HK")
        track_stock.drop_duplicates(subset='wind_code', inplace=True)
        track_stock.set_index('wind_code', drop=False, inplace=True)

        wind_code_str = ",".join(track_stock['wind_code'].to_list())
        wind_field = 'est'
        new_rpt_count = 0

        for id_broker in range(1, ID_BROKER_MAX + 1):
            cmd = est_refresh_time_cmd.replace('WIND_CODE_LIST', wind_code_str).replace('BROKER', str(id_broker))
            w_result = eval(cmd)
            data = w_result[1]

            data[data < datetime(2010, 1, 1)] = np.nan
            data.columns = [str.lower(x) for x in list(data.columns)]
            data.rename(columns=wind_shszhk_field_in_db_replace[wind_field], inplace=True)
            data = data[(data['new_est_time'] >= (datetime.now() - timedelta(days=1)))
                        | (data['new_rating_time'] >= (datetime.now() - timedelta(days=1)))]

            if not data.empty:
                new_rpt_count = new_rpt_count + len(data)
                print(f'broker{id_broker}今天发布{len(data)}篇研报!*********************')
                main_field = wind_shszhk_field_in_db_replace[wind_field].values()
                data.loc[:, 'wind_code'] = data.index
                data.loc[:, 'sec_name'] = track_stock['sec_name']
                data.loc[:, 'organ_id'] = id_broker
                data.dropna(subset=main_field, how="all", inplace=True)

                for i, row in data.iterrows():
                    sql = f"delete from est_rpt_refresh_time_stk where organ_id={id_broker} and wind_code='{row['wind_code']}' "
                    DBTool.dbcur.execute(sql)

                data.to_sql("est_rpt_refresh_time_stk", con=DBTool.dbe, if_exists="append", index=False)
            else:
                print(f'broker{id_broker}')

        now_date = f'{datetime.now().date()}'.replace('-', '')
        sql = f"select wind_code,sec_name,organ_id,new_est_time,new_rating_time from est_rpt_refresh_time_stk  " \
              f"where date_add(new_est_time, interval 1 day)>='{now_date}' or date_add(new_rating_time, interval 1 day)>='{now_date}'"
        rpt_info = pd.read_sql_query(sql=sql, con=DBTool.dbc)

        # 判断是否有需要拉取的数据
        if not rpt_info.empty:
            print(f'准备拉取今天新发布{len(rpt_info)}篇研报!**********************')

            rpt_info.set_index('wind_code', drop=False, inplace=True)
            broker_list = list(set(rpt_info['organ_id']))

            for id_broker in broker_list:
                try:
                    data_all = pd.DataFrame()
                    organ_stk = rpt_info[rpt_info['organ_id'] == id_broker]
                    wind_code_str = ",".join(organ_stk['wind_code'].to_list())
                    print(f'broker{id_broker}今天发布{len(organ_stk)}篇研报!')
                    # print(f"正在拉取预测年度为{year},trade_date为{trade_date},broker为{id_broker}的最新预测研报时间...")
                    for est_year in range(int(year) - 1, int(year) + 2):
                        print(f"正在拉取id_broker为{id_broker}的研报详情,年度为{est_year}")

                        if est_year == int(year) - 1:
                            cmd = est_rpt_details_cmd \
                                .replace('WIND_CODE_LIST', wind_code_str) \
                                .replace('BROKER', str(id_broker)) \
                                .replace('YEAR', str(est_year))
                        else:
                            cmd = est_rpt_details_cmd \
                                .replace('WIND_CODE_LIST', wind_code_str) \
                                .replace('BROKER', str(id_broker)) \
                                .replace('YEAR', str(est_year)) \
                                .replace(
                                ",est_orgrating_inst,est_scorerating_inst,est_stdrating_inst,est_highprice_inst,est_lowprice_inst,est_prehighprice_inst,est_prelowprice_inst,est_frstratingtime_inst,est_ratinganalyst,est_estanalyst,est_pctchange,est_newratingtime_inst,est_estnewtime_inst",
                                "").replace("est_rpttitle_inst,est_rptabstract_inst,", "")

                        w_result = eval(cmd)
                        data = w_result[1]
                        data.columns = [str.lower(x) for x in list(data.columns)]
                        data.columns = [x.replace("_inst", "") for x in list(data.columns)]

                        suffix = '_FY' + str(est_year - int(year) + 2)
                        replace_fields = {}
                        for field in ['est_netprofit', 'est_eps', 'est_sales']:
                            replace_fields[field] = field + suffix
                        data.rename(columns=replace_fields, inplace=True)

                        if est_year == int(year) - 1:
                            data.loc[data['est_newratingtime'] < datetime(2010, 1, 1), 'est_newratingtime'] = np.nan
                            data.loc[data['est_frstratingtime'] < datetime(1900, 1, 1), 'est_frstratingtime'] = np.nan
                            data.loc[data['est_estnewtime'] < datetime(2010, 1, 1), 'est_estnewtime'] = np.nan
                            data.loc[~data['est_estnewtime'].isnull(), 'date'] = data['est_estnewtime']
                            data.loc[data['est_estnewtime'].isnull(), 'date'] = data['est_newratingtime']
                            data['sec_name'] = organ_stk['sec_name']
                            data['wind_code'] = data.index
                            est_orgrating = data.loc[~data['est_orgrating'].isnull(), 'est_orgrating'].iloc[0]
                            organ_name = est_orgrating[:est_orgrating.index("_")]
                            data['organ_name'] = organ_name
                            data['organ_id'] = id_broker

                        data_all = data.join(data_all, how='left')

                    if not data_all.empty:
                        for i, row in data_all.iterrows():
                            sql = f"delete from est_rpt_details_stk where organ_id={id_broker} and wind_code='{row['wind_code']}' " \
                                  f"and date='{row['date']}' "
                            DBTool.dbcur.execute(sql)

                        data_all.to_sql("est_rpt_details_stk", con=DBTool.dbe, if_exists="append", index=False)

                except Exception as e:
                    print(str(e))
        else:
            print(f'今天没有发布研报...,无需拉取研报详情')


    @classmethod
    def __sync_common_data(cls):

        for table_key, table_name in table_replace.items():
            table_last_date = DBTool.get_last_date(table_name=table_name)

            [DBTool.manual_fetch_wind_data(w_cmd={
                'wind_field': table_key,
                'request_date': (table_last_date + timedelta(i)).strftime("%Y%m%d")
            }) for i in range((date.today() - table_last_date).days + 1)]


if __name__ == '__main__':
    # [DBDataManager.sync_data_to_db(sync_type=sync_type) for sync_type in ['common', 'wind_report']]
    # [DBDataManager.sync_data_to_db(sync_type=sync_type) for sync_type in ['common']]

    w.start(showmenu=False)
    _wind_cmd = 'w.wsd("000001.SZ", "close,volume", "2023-03-28", "2023-04-26", "")'
    _wind_ret = eval(_wind_cmd)
    print("_wind_ret: " + str(_wind_ret))
    print("_wind_ret Codes: " + str(_wind_ret.Codes))
    print("_wind_ret Fields: " + str(_wind_ret.Fields))
    print("_wind_ret Times: " + str(_wind_ret.Times))
    print("_wind_ret Data: " + str(_wind_ret.Data))
    print("type _wind_ret: " + str(type(_wind_ret)))

    w.WindData()
