import os

from util import *


def prepocess(raw_corpus_file_name, result_file_name):
    raw_corpus_file = codecs.open(raw_corpus_file_name, encoding=Config.encoding, errors="replace")

    file_num = 0

    file_name = result_file_name.replace(".tsv", "-" + str(file_num) + ".tsv")
    result = codecs.open(file_name, "w", encoding=Config.encoding)

    for index, line in enumerate(raw_corpus_file):
        if index % 100000 == 0:
            print(raw_corpus_file_name, index)
            if (index % 600000 == 0) & (index > 0):
                result.close()
                format_refine(file_name)
                file_num += 1
                file_name = result_file_name.replace(".tsv", "-" + str(file_num) + ".tsv")
                result = codecs.open(file_name, "w", encoding=Config.encoding)

        pair = line.strip().split()
        result.write("\t".join(pair) + "\n")

    raw_corpus_file.close()
    result.close()


def tieba_process_pipeline():
    print("tieba_process_pipeline")

    raw_corpus_file_name = Config.raw_tieba_corpus_path
    result_file_name = os.path.join(Config.clean_chat_corpus_root, "tieba.tsv")
    prepocess(raw_corpus_file_name, result_file_name)
