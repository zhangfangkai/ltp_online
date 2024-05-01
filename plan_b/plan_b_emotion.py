import csv
import os
import re
from collections import Counter

import jieba
import pandas as pd

from mysql_helper import MysqlHelper


def statictic_chars(text):
    chinese_chars = re.findall(r'[\u4e00-\u9fff]', text)
    english_words = re.findall(r'\b[a-zA-Z]+\b', text)

    num_chinese_chars = len(chinese_chars)
    num_english_words = len(english_words)

    return num_chinese_chars, num_english_words


def statictic_emotion(fenci_result):
    negative = ciku_dict["negative"]
    positive = ciku_dict["positive"]
    positive_num = 0
    negtive_num = 0

    for i in range(len(fenci_result)):
        # get single fenci
        fenci_single = fenci_result[i]
        # statictic postive negative count
        for item in fenci_single:
            if item in negative:
                negtive_num += 1
            if item in positive:
                positive_num += 1
    return positive_num, negtive_num


def split_sentence():
    # split_sentence
    data_dir = "data/cmda_fix"
    jieba.load_userdict("data/emotion_cidian/custom_dict.txt")
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
            total_sentence_count = len(sentences)
            num_chinese_chars, num_english_words = statictic_chars(paragraph)
            num_all_chars = num_chinese_chars + num_english_words
            num_positive, num_negtive = statictic_emotion(fenci_result)
        stock_code = file_path.split('/')[-1].split('_')[0]
        # year
        year = file_path.split('/')[-1].split('_')[1].split(".")[0]

        mysql_helper.insert_emotion_res(stock_code,year,total_sentence_count, num_chinese_chars, num_english_words, num_all_chars, num_positive, num_negtive)
        # print(stock_code,year,total_sentence_count, num_chinese_chars, num_english_words, num_all_chars, num_positive, num_negtive)
        print("---{} done---".format(file_path))


def clean_dict():
    # each dict is unique
    data_dir = "data/emotion_cidian"
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
    data_dir = "data/emotion_cidian"

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


if __name__ == '__main__':
    # init mysql
    mysql_helper = MysqlHelper()
    ciku_dict = dict()
    load_dict()
    split_sentence()
    mysql_helper.close()
