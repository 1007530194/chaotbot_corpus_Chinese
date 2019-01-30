import os

from util import *


def preprocess(raw_corpus_post_file_name, raw_corpus_response_file_name, result_file_name):
    raw_corpus_post_file = codecs.open(raw_corpus_post_file_name, encoding=Config.encoding)
    raw_corpus_response_file = codecs.open(raw_corpus_response_file_name, encoding=Config.encoding)

    file_num = 0
    file_name = result_file_name.replace(".tsv", "-" + str(file_num) + ".tsv")
    result = codecs.open(file_name, "w", encoding=Config.encoding)

    for index, pair in enumerate(zip(raw_corpus_post_file, raw_corpus_response_file)):
        if index % 100000 == 0:
            print(raw_corpus_post_file_name, raw_corpus_response_file_name, index)
            if (index % 900000 == 0) & (index > 0):
                result.close()
                format_refine(file_name)
                file_num += 1
                file_name = result_file_name.replace(".tsv", "-" + str(file_num) + ".tsv")
                result = codecs.open(file_name, "w", encoding=Config.encoding)

        post = pair[0].strip().replace(" ", "")
        response = pair[1].strip().replace(" ", "")

        result.write(post + "\t" + response + "\n")

    raw_corpus_post_file.close()
    raw_corpus_response_file.close()
    result.close()


def weibo_process_pipeline():
    print("weibo_process_pipeline")
    raw_corpus_post_file_name = Config.raw_weibo_post_corpus_path
    raw_corpus_response_file_name = Config.raw_weibo_response_corpus_path
    result_file_name = os.path.join(Config.clean_chat_corpus_root, "weibo.tsv")

    preprocess(raw_corpus_post_file_name, raw_corpus_response_file_name, result_file_name)
