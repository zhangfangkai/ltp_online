import csv
import os
import re
from collections import Counter

import jieba
import pandas as pd

from mysql_helper import MysqlHelper


def out_put(res):
    file_name = "./data/out_put/plan_b_res.csv"
    with open(file_name, 'a', encoding="utf-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(res)

def statictic_last_four(stock_code, year, index, total_sentence_count, total_fenci_count, yali_count, fenci_single):
    qianzhanxing = ciku_dict["10_qianzhanxing"]
    huanjinzhengce = ciku_dict["7_huanjingzhengce"]
    chanpinjingzheng = ciku_dict["8_chanpinjingzheng"]
    qiyetouzi = ciku_dict["9_qiyetouzizhe"]
    qianzhanxing_flag = 0
    huanjinzhengce_flag = 0
    chanpinjingzheng_flag = 0
    qiyetouzi_flag = 0
    for item in fenci_single:
        if item in qianzhanxing:
            qianzhanxing_flag = 1
        if item in huanjinzhengce:
            huanjinzhengce_flag = 1
        if item in chanpinjingzheng:
            chanpinjingzheng_flag = 1
        if item in qiyetouzi:
            qiyetouzi_flag = 1

    mysql_helper.insert_res(stock_code, year, index, total_sentence_count, total_fenci_count, yali_count, qianzhanxing_flag, huanjinzhengce_flag,
          chanpinjingzheng_flag, qiyetouzi_flag,fenci_single)
    # print(stock_code, year, index, total_sentence_count, total_fenci_count, yali_count, qianzhanxing_flag, huanjinzhengce_flag,
    #      chanpinjingzheng_flag, qiyetouzi_flag, "fenci_single")




def statictic_yali(stock_code, year, index, total_sentence_count, total_fenci_count, fenci_single):

    yali = ciku_dict["6_yalibiao"]
    yali_count = 0
    for item in fenci_single:
        if item in yali:
            yali_count += 1
    if yali_count > 0:
        statictic_last_four(stock_code, year, index, total_sentence_count, total_fenci_count, yali_count, fenci_single)
    else:
        mysql_helper.insert_res(stock_code, year, index, total_sentence_count, total_fenci_count, yali_count, 0, 0, 0, 0,
                          fenci_single)
        # print(stock_code, year, index, total_sentence_count, total_fenci_count, yali_count, 0, 0, 0, 0, "fenci_single")


def statictic(file_path, sentences, fenci_result):

    stock_code = file_path.split('/')[-1].split('_')[0]
    year = file_path.split('/')[-1].split('_')[1].split(".")[0]

    total_sentence_count = len(sentences)

    total_fenci_count = 0
    for fenci_single in fenci_result:
        total_fenci_count += len(fenci_single)

    huanbao = ciku_dict["5_huanbaociku"]
    for i in range(len(fenci_result)):
        fenci_single = fenci_result[i]
        flag = 0

        for item in fenci_single:
            if item in huanbao:
                flag = 1
                break

        if flag == 1:
            statictic_yali(stock_code, year, i, total_sentence_count, total_fenci_count, fenci_single)


def split_sentence():
    # split_sentence
    data_dir = "./data/raw_cmda_txt"
    jieba.load_userdict("./data/huanbao_cidian/custom_dict.txt")
    file_list = os.listdir(data_dir)
    for file_name in file_list:
        # only 12-31
        if "12-31" not in file_name:
            continue
        file_path = os.path.join(data_dir, file_name)
        with (open(file_path, "r")) as f:
            paragraph = f.readlines()[0].strip()

            sentences = re.split(r'(?<=[。！？])', paragraph)

            fenci_result = []
            for sentence in sentences:
                sentence = sentence.strip()
                seg_list = jieba.cut(sentence, cut_all=False)
                tmp = [i for i in seg_list]
                fenci_result.append(tmp)
            statictic(file_path, sentences, fenci_result)
        print(file_path, "done")


def init_dict():
    data_dir = "data/huanbao_cidian"

    ciku_dict = dict()
    file_list = os.listdir(data_dir)
    res = []
    for file_name in file_list:
        if file_name == "custom_dict.txt":
            continue
        key_name = file_name.split('.')[0]
        file_path = os.path.join(data_dir, file_name)
        single_res = []
        with (open(file_path, "r")) as f:
            for line in f:
                line = line.strip()
                single_res.append(line)
                res.append(line)
        ciku_dict[key_name] = single_res

    counter = Counter(res)
    counter_res = counter.most_common(44)
    for key, value in counter_res:
        tmp_list = []
        for k, v in ciku_dict.items():
            if key in v:
                tmp_list.append(k)
        # print(key, value, tmp_list)


def gen_custom_dict():
    data_dir = "data/huanbao_cidian"
    file_list = os.listdir(data_dir)
    res = []
    for file_name in file_list:
        file_path = os.path.join(data_dir, file_name)
        with (open(file_path, "r")) as f:
            for line in f:
                line = line.strip()
                res.append(line)
    with open("data/huanbao_cidian/custom_dict.txt", 'a') as f:
        f.writelines([line + '\n' for line in res])


def clean_dict():
    # each dict is unique
    data_dir = "data/huanbao_cidian"
    file_list = os.listdir(data_dir)
    for file_name in file_list:
        if file_name == "custom_dict.txt":
            continue
        key_name = file_name.split('.')[0]
        file_path = os.path.join(data_dir, file_name)
        single_res = []
        with (open(file_path, "r")) as f:
            for line in f:
                line = line.strip()
                if line not in single_res:
                    single_res.append(line)
        new_dir = "./data/cidian_new"
        new_path = os.path.join(new_dir, file_name)
        # print(new_path)
        with open(new_path, "a", encoding="utf-8") as f:
            f.writelines([line + '\n' for line in single_res])


def load_dict():
    # load all dict to ciku_dict
    data_dir = "data/huanbao_cidian"

    file_list = os.listdir(data_dir)
    for file_name in file_list:
        # filter custom dict
        if file_name == "custom_dict.txt":
            continue
        key_name = file_name.split('.')[0]
        file_path = os.path.join(data_dir, file_name)
        single_res = []
        with (open(file_path, "r")) as f:
            for line in f:
                line = line.strip()
                single_res.append(line)
        ciku_dict[key_name] = single_res


def init_out_put():
    # init new file
    all_type = list(ciku_dict.keys())
    all_type.sort()
    res = ["stock_code", "year"] + all_type + ["total_sentence_count", "sentences", "fenci_result"]
    file_name = "./data/out_put/plan_b_res.csv"
    with open(file_name, 'w', encoding="utf-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(res)


def convert_to_xlsx():
    df = pd.read_csv("./data/out_put/plan_b_res.csv")
    df.to_excel("./data/out_put/plan_b_res.xlsx", index=True)


if __name__ == '__main__':
    # init mysql
    mysql_helper = MysqlHelper()
    ciku_dict = dict()

    load_dict()

    split_sentence()

    mysql_helper.close()
