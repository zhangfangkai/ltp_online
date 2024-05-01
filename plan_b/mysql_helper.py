import pymysql


class MysqlHelper:
    def __init__(self):
        self.db = pymysql.connect(
            # host='114.115.132.201',
            host='127.0.0.1',
            user='root',
            password='710218',
            database='ltp')
    
    def insert_inno_res(self, stock_code, year, index_num, total_sentence_count, chuangxin_flag, hezuo_flag,
                        shuzihua_flag, gongyinglian_flag):
        cursor = self.db.cursor()
        # SQL 插入语句
        sql = """INSERT INTO planb_res_innovation(`stock_code`, `year`, `index_num`, `total_sentence_count`, `chuangxin_flag`, 
           `hezuo_flag`, `shuzihua_flag`, `gongyinglian_flag`) VALUES ('%s', '%s', %d, %d, %d, %d, %d, %d)
           """ % (
        stock_code, year, index_num, total_sentence_count, chuangxin_flag, hezuo_flag, shuzihua_flag, gongyinglian_flag)
        # print(sql)
        try:
            # 执行sql语句
            cursor.execute(sql)
            # 提交到数据库执行
            self.db.commit()
        except Exception as e:
            # 如果发生错误则回滚
            print(e)
            self.db.rollback()

    def insert_res(self, stock_code, year, index_num, total_sentence_count, total_fenci_count, yali_count,
                   qianzhanxing_flag, huanjinzhengce_flag, chanpinjingzheng_flag, qiyetouzi_flag, fenci_single):
        cursor = self.db.cursor()
        fenci_text = "/".join(fenci_single)
        # SQL 插入语句
        sql = """INSERT INTO planb_res(`stock_code`, `year`, `index_num`, `total_sentence_count`, `total_fenci_count`, 
        `yali_count`, `qianzhanxing_flag`, `huanjinzhengce_flag`, `chanpinjingzheng_flag`, 
        `qiyetouzi_flag`, `fenci_res`) VALUES ('%s', '%s', %d, %d, %d, %d, %d, %d, %d, %d, '%s')
        """ % (stock_code, year, index_num, total_sentence_count, total_fenci_count, yali_count,
                   qianzhanxing_flag, huanjinzhengce_flag, chanpinjingzheng_flag, qiyetouzi_flag, fenci_text)
        # print(sql)
        try:
            # 执行sql语句
            cursor.execute(sql)
            # 提交到数据库执行
            self.db.commit()
        except Exception as e:
            # 如果发生错误则回滚
            print(e)
            self.db.rollback()

    def insert_emotion_res(self, stock_code, year, total_sentence_count, num_chinese_chars, num_english_words, num_all_chars,
                           num_positive, num_negtive):

        cursor = self.db.cursor()
        # SQL 插入语句
        sql = """INSERT INTO planb_res_tone(`stock_code`, `year`, `total_sentence_count`, `num_chinese_chars`, 
           `num_english_words`, `num_all_chars`, `num_positive`,`num_negtive`) VALUES ('%s','%s', %d, %d, %d, %d, %d, %d)
           """ % (
            stock_code, year, total_sentence_count, num_chinese_chars, num_english_words, num_all_chars,
            num_positive, num_negtive)
        # print(sql)
        try:
            # 执行sql语句
            cursor.execute(sql)
            # 提交到数据库执行
            self.db.commit()
        except Exception as e:
            # 如果发生错误则回滚
            print(e)
            self.db.rollback()

    def close(self):
        self.db.close()
