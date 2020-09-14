# coding=utf-8
"""
created on 2020/07/20
@author:gmcc_cbl & zs

"""

import datetime
import re
import os
import pandas as pd
from string import digits


sigstand_db_name = "中国移动动环信号标准化字典表"
alastand_db_name = "中国移动动环告警标准化字典表"
ala_dic_name = '告警标准名'
ala_dic_key = '是否关键信号'
sig_dic_name = '信号标准名'
sig_dic_key = '是否关键信号'
alarmtitle_name = 'AlarmTitleDes'
aic_node = 'NodeId'
aic_name = 'NodeName'
dals_node = 'NodeId'
empty_name = ''


# 信号量规范性勘误
def sigstand_corr():
    log_flag = 1
    log_flag_list = [
        100,
        200,
        300,
        400,
        500,
        600,
        700,
        800,
        900,
        1000]
    print("开始读取数据……")
    dal_df = pd.read_excel(path_target, sheet_name='T_AIStatic').drop_duplicates([dals_node])
    aic_df = pd.read_excel(path_target, sheet_name='M_AIC')
    nodeid = dal_df['NodeId'].tolist()
    aic_df = aic_df[aic_df['NodeId'].isin(nodeid)]
    sig_df = pd.read_excel(path_dict, sheet_name=sigstand_db_name)
    aic_df2 = aic_df.drop_duplicates([aic_name])
    log1 = "去重后信号量数共%d条\n" % len(aic_df2)
    print(log1)
    print("开始进行信号量规范性勘误……")

    sig_stand_df = pd.DataFrame(
        columns=(
            'NodeID',
            'NodeName',
            '信号标准名',
            '是否符合标准名'))

    # 通过去重后的AIC表的每一条信号标题去比对标准表
    for aic_index in aic_df2.index:
        a_node = aic_df2.loc[aic_index][aic_node]
        a_name = aic_df2.loc[aic_index][aic_name]
        remove_digits = str.maketrans('', '', digits)
        res = a_name.translate(remove_digits)
        a_res = re.sub('\\(.*?\\)', '', res)
        b = 0
        for sig_index in sig_df.index:
            sig_name = sig_df.loc[sig_index][sig_dic_name]
            a = sig_name.replace("XX", "")
            if a_res == a:
                save_name = '是'
                sig_stand_df = sig_stand_df.append(
                    pd.DataFrame(
                        {
                            'NodeID': [a_node],
                            'NodeName': [a_name],
                            '信号标准名': [sig_name],
                            '是否符合标准名': [save_name],
                        }))
                b = b + 1
                break
        if b == 0:
            save_name = '否'
            sig_stand_df = sig_stand_df.append(
                pd.DataFrame(
                    {
                        'NodeID': [a_node],
                        'NodeName': [a_name],
                        '信号标准名': [empty_name],
                        '是否符合标准名': [save_name],
                    }))
        log_flag = log_flag + 1
        if log_flag in log_flag_list:
            print("【已勘误信号量数】：", log_flag)
    sig_stand_df.to_excel('信号量规范性勘误.xlsx', index=None)
    print("\n")
    print("信号量规范性勘误完成！数据保存到：信号量规范性勘误.xlsx")


# BID与NodeID转换
def BIDtoNodeID(BID):
    NodeId = 0
    binary_ID = bin(BID)[:-11]
    b = binary_ID + '00000000000'
    for index, i in enumerate(b[2:]):
        NodeId = NodeId + int(i) * (2 ** (len(b[2:]) - 1 - index))
    return NodeId


def device_type_search():
    df_trgt_alarm['NodeId'] = df_trgt_alarm['BID'].apply(BIDtoNodeID)
    df_device_search = pd.merge(df_trgt_alarm, df_trgt_device.loc[:, [
        'NodeId', 'DeviceType']], how='left',
        on='NodeId')
    return df_device_search


# 告警标题规范性勘误
def search_correct(df, df_standard):
    log_flag = 1
    log_flag_list = [
        10000,
        20000,
        30000,
        40000,
        50000,
        60000,
        70000,
        80000,
        90000,
        100000]
    print("开始进行告警标题规范性勘误……")
    df2 = df_trgt_alarm.drop_duplicates(['BID'])
    log1 = "去重后告警标题数共%d条\n" % len(df2)
    print(log1)
    df['TitleFilted'] = df['AlarmTitleDes'].apply(
        lambda x: re.sub(regex1, 'X', x))
    df['TitleFilted'] = df['TitleFilted'].apply(
        lambda x: re.sub(regex2, 'x', x))
    df['TitleFilted'] = df['TitleFilted'].apply(
        lambda x: re.sub(regex3, '', x))

    df_search = pd.merge(df,
                         df_standard.loc[:,
                                         ['第二、三位（设备类型编码）',
                                          '告警标准名']],
                         how='left',
                         left_on='TitleFilted',
                         right_on='告警标准名')
    df_correction_on_device = pd.DataFrame(
        data=None,
        columns=(
            'NodeId',
            'BID',
            'title',
            'DeviceType',
            '第二、三位（设备类型编码）',
            '告警标准名'))

    BID = df['BID']
    for j in BID:
        tmp = df_search[df_search['BID'].values == j]
        find = False
        for k in tmp.itertuples():
            if getattr(k, 'DeviceType') == getattr(k, '_11'):
                find = True
                break
        if not find:
            dic_temp = {
                'NodeId': getattr(
                    k, 'NodeId'), 'BID': getattr(
                    k, 'BID'), 'title': getattr(
                    k, 'AlarmTitleDes'), 'DeviceType': getattr(
                    k, 'DeviceType'), '第二、三位（设备类型编码）': getattr(
                    k, '_11'), '告警标准名': getattr(
                    k, '告警标准名')}
            df_correction_on_device = df_correction_on_device.append(
                dic_temp, ignore_index=True)
        log_flag = log_flag + 1
        if log_flag in log_flag_list:
            print("【已勘误告警标题数】：", log_flag)
    df_correction_titles = df_correction_on_device[df_correction_on_device['告警标准名'].isnull(
    ).values]
    df_device_error = df_correction_on_device[df_correction_on_device['告警标准名'].isnull(
    ).values == False]
    print("告警标题规范性勘误完成！数据保存到：告警标题规范性勘误.xlsx")
    return df_device_error, df_correction_titles, df_search


# 信号量、告警标题完整性勘误
def comple_corr():
    print("下面开始进行信号量完整性勘误……\n")
    sig_df = pd.read_excel(path_dict, sheet_name=sigstand_db_name)
    alastan_df = pd.read_excel(path_dict, sheet_name=alastand_db_name)
    aic_df = pd.read_excel(path_target, sheet_name='M_AIC')
    ala_df = pd.read_excel(path_target, sheet_name='AlarmTitle')
    # 筛选出完整性告警名和信号名
    sig_df2 = sig_df[sig_df['完整性勘误'].isin(['完整性'])]
    alastan_df2 = alastan_df[alastan_df['完整性勘误'].isin(['完整性'])]
    # 给M_AIC表信号名去重
    aic_df2 = aic_df.drop_duplicates([aic_name])
    # 给alarmtitle表告警名去重
    ala_df2 = ala_df.drop_duplicates([alarmtitle_name])

    log1 = "信号标准名数共%d条\n" % len(sig_df2)
    print(log1)

    sigcom_cor_df = pd.DataFrame(
        columns=(
            '信号标准名',
            'NodeName',
            '是否符合完整性',
            '是否关键信号'))

    alacom_cor_df = pd.DataFrame(
        columns=(
            '告警标准名',
            'NodeName',
            '是否符合完整性',
            '是否关键信号'))

    # 设置一个勘误次数反馈
    log_flag = 1
    log_flag_list = [
        50,
        100,
        150,
        200]

    # 信号量完整性勘误
    for sig_index in sig_df2.index:
        sig_name = sig_df2.loc[sig_index][sig_dic_name]
        key_name = sig_df2.loc[sig_index][sig_dic_key]
        a = sig_name.replace("XX", "")
        b = 0
        for aic_index in aic_df2.index:
            a_name = aic_df2.loc[aic_index][aic_name]
            remove_digits = str.maketrans('', '', digits)
            res = a_name.translate(remove_digits)
            a_res = re.sub('\\(.*?\\)', '', res)
            if a_res == a:
                save_name = '已有'
                sigcom_cor_df = sigcom_cor_df.append(
                    pd.DataFrame(
                        {
                            '信号标准名': [sig_name],
                            'NodeName': [a_name],
                            '是否符合完整性': [save_name],
                            '是否关键信号': [key_name],
                        }))
                b = b + 1
                break
        if b == 0:
            save_name = '缺失'
            sigcom_cor_df = sigcom_cor_df.append(
                pd.DataFrame(
                    {
                        '信号标准名': [sig_name],
                        'NodeName': [empty_name],
                        '是否符合完整性': [save_name],
                        '是否关键信号': [key_name],
                    }))
        log_flag = log_flag + 1
        if log_flag in log_flag_list:
            print("【已勘误信号标准名数】：", log_flag)
    sigcom_cor_df.to_excel('信号量完整性勘误.xlsx', index=None)
    print("\n")
    print("信号量完整性勘误完成！数据保存到：信号量完整性勘误.xlsx")
    # 告警完整性勘误
    print("\n")
    print("下面开始进行告警标题完整性勘误……\n")

    log2 = "告警标题标准名数共%d条\n" % len(alastan_df2)
    print(log2)

    # 重置勘误数
    log_flag = 1

    # 告警标题完整性勘误
    for ala_index in alastan_df2.index:
        ala_name = alastan_df2.loc[ala_index][ala_dic_name]
        key_name = alastan_df2.loc[ala_index][ala_dic_key]
        a = ala_name.replace("XX", "")
        b = 0
        for title_index in ala_df2.index:
            a_name = ala_df2.loc[title_index][alarmtitle_name]
            remove_digits = str.maketrans('', '', digits)
            res = a_name.translate(remove_digits)
            a_res = re.sub('\\(.*?\\)', '', res)
            a_res = a_res.replace("a", "x").replace("b", "x").replace("c", "x")
            if a_res == a:
                save_name = '已有'
                alacom_cor_df = alacom_cor_df.append(
                    pd.DataFrame(
                        {
                            '告警标准名': [ala_name],
                            'NodeName': [a_name],
                            '是否符合完整性': [save_name],
                            '是否关键信号': [key_name],
                        }))
                b = b + 1
                break
        if b == 0:
            save_name = '缺失'
            alacom_cor_df = alacom_cor_df.append(
                pd.DataFrame(
                    {
                        '告警标准名': [ala_name],
                        'NodeName': [empty_name],
                        '是否符合完整性': [save_name],
                        '是否关键信号': [key_name],
                    }))
        log_flag = log_flag + 1
        if log_flag in log_flag_list:
            print("【已勘误告警标准名数】：", log_flag)
    alacom_cor_df.to_excel('告警标题完整性勘误.xlsx', index=None)
    print("\n")
    print("告警标题完整性勘误完成！数据保存到：告警标题完整性勘误.xlsx")


"""---------------------------------
主函数~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
---------------------------------"""
starttime = datetime.datetime.now()
print("欢迎使用动环勘误工具！")
try:
    path_target = input('请输入完整的数据文件路径：')
    path_dict = input('标准表路径：')
except FileNotFoundError as FileError:
    print('未找到指定文件，请检查输入路径是否正确')
sigstand_corr()
df_trgt_alarm = pd.read_excel(path_target, sheet_name='AlarmTitle')
# 去除重复BID值
df_trgt_alarm = df_trgt_alarm.drop_duplicates(['BID'])
df_trgt_device = pd.read_excel(path_target, sheet_name='M_Device')
df_dict = pd.read_excel(path_dict, sheet_name=alastand_db_name)
# 正则匹配规则
regex1 = re.compile(r'\d')
regex2 = re.compile(r'(a|b|c)')
regex3 = re.compile(r'[（].*[）]')

df_device_include = device_type_search()
df_error_device, df_error_titles, df_result = search_correct(
    df_device_include, df_dict)
sheets = ('df_error_device', 'df_error_titles', 'df_result')

with pd.ExcelWriter(os.getcwd() + '\\告警标题规范性勘误.xlsx') as writer:
    df_error_device.to_excel(writer, sheet_name='df_error_device', index=None)
    df_error_titles.to_excel(writer, sheet_name='df_error_titles', index=None)
comple_corr()
endtime = datetime.datetime.now()
print("所有勘误完成！")
print("程序总耗时：[", starttime, endtime, "]:", (endtime - starttime))
