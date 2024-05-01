import csv
import os
import re
from collections import Counter

import jieba
import pandas as pd

from mysql_helper import MysqlHelper


def statictic_innovation(file_path, fenci_result):
    # stock_code
    stock_code = file_path.split('/')[-1].split('_')[0]
    # year
    year = file_path.split('/')[-1].split('_')[1].split(".")[0]

    total_sentence_count = len(fenci_result)
    chuangxin = ciku_dict["5_chuangxin"]
    hezuo = ciku_dict["6_hezuo"]
    shuzihua = ciku_dict["7_shuzihua"]
    gongyinglian = ciku_dict["8_gongyinglian"]

    for i in range(len(fenci_result)):
        # get single fenci
        flag_5 = 0
        flag_6 = 0
        flag_7 = 0
        flag_8 = 0
        flag_total = 0
        fenci_single = fenci_result[i]
        for item in fenci_single:
            if flag_5 == 0 and item in chuangxin:
                flag_5 = 1
                flag_total += 1
                if flag_total >= 4:
                    break
            if flag_6 == 0 and item in hezuo:
                flag_6 = 1
                flag_total += 1
                if flag_total >= 4:
                    break
            if flag_7 == 0 and item in shuzihua:
                flag_7 = 1
                flag_total += 1
                if flag_total >= 4:
                    break
            if flag_8 == 0 and item in gongyinglian:
                flag_8 = 1
                flag_total += 1
                if flag_total >= 4:
                    break
        # print(stock_code, year, i, "fenci_single", total_sentence_count, flag_5, flag_6, flag_7, flag_8)
        if flag_total > 0:
            mysql_helper.insert_inno_res(stock_code, year, i, total_sentence_count, flag_5, flag_6, flag_7, flag_8)


def split_sentence():
    # split_sentence
    data_dir = "data/cmda_fix"
    jieba.load_userdict("data/innovation_cidian/custom_dict.txt")
    file_list = os.listdir(data_dir)
    for file_name in file_list:
        # only 12-31
        if "12-31" not in file_name:
            continue
        print("---{} start---".format(file_name))
        file_path = os.path.join(data_dir, file_name)
        with (open(file_path, "r")) as f:
            paragraph = f.readlines()[0].strip()
            # fenju
            sentences = re.split(r'(?<=[。！？])', paragraph)
            # fenci
            fenci_result = []
            for sentence in sentences:
                sentence = sentence.strip()
                seg_list = jieba.cut(sentence, cut_all=False)
                tmp = [i for i in seg_list]
                fenci_result.append(tmp)

            statictic_innovation(file_path, fenci_result)

        # print("---{} done---".format(file_path))


def clean_dict():
    # each dict is unique
    data_dir = "../data/emotion_cidian"
    file_list = os.listdir(data_dir)
    for file_name in file_list:
        if file_name == "custom_dict.txt":
            continue
        file_path = os.path.join(data_dir, file_name)
        single_res = []
        print(file_path)
        with (open(file_path, "r")) as f:
            for line in f:
                line = line.strip()
                non_chinese_chars = re.findall(r'[^\u4e00-\u9fff]+', line)
                if non_chinese_chars:
                    print(line)
                single_res.append(line)
        print(len(single_res), len(set(single_res)))
        print(Counter(single_res).most_common(10))


def load_dict():
    # load all dict to ciku_dict
    data_dir = "data/innovation_cidian"

    file_list = os.listdir(data_dir)
    for file_name in file_list:
        # filter custom dict
        file_name_prefix = file_name.split("_")[0]
        if file_name == "custom_dict.txt" or int(file_name_prefix) < 5:
            continue
        key_name = file_name.split('.')[0]
        file_path = os.path.join(data_dir, file_name)
        single_res = []
        with (open(file_path, "r")) as f:
            for line in f:
                line = line.strip()
                single_res.append(line)
        ciku_dict[key_name] = single_res


if __name__ == '__main__':
    # init mysql

    mysql_helper = MysqlHelper()
    ciku_dict = dict()
    load_dict()
    split_sentence()
    mysql_helper.close()
