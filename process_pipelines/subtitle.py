import os

from util import *


def prepocess(raw_corpus_file_name, result_file_name):
    start_end_symbol = "E"
    utterance_symbol = "M"

    raw_corpus_file = codecs.open(raw_corpus_file_name, encoding=Config.encoding, errors="replace")

    file_num = 0

    file_name = result_file_name.replace(".tsv", "-" + str(file_num) + ".tsv")
    result = codecs.open(file_name, "w", encoding=Config.encoding)

    single_session = []
    session_lengths = []

    for index, line in enumerate(raw_corpus_file):
        if index % 100000 == 0:
            print(raw_corpus_file_name, index)
            if (index % 2000000 == 0) & (index > 0):
                result.close()
                format_refine(file_name)
                file_num += 1
                file_name = result_file_name.replace(".tsv", "-" + str(file_num) + ".tsv")
                result = codecs.open(file_name, "w", encoding=Config.encoding)

        if line.startswith(start_end_symbol):
            if len(single_session) > 1:
                pairs = generate_single_pairs_from_multi_turn(single_session)
                for pair in pairs:
                    result.write("\t".join(pair) + "\n")
                session_lengths.append(len(single_session))
            single_session = []
        elif line.startswith(utterance_symbol):
            line = line[1:].strip()
            utterance = line.replace("/", "").strip()
            single_session.append(utterance)
        else:
            print(line)

    print("avg session length", sum(session_lengths) / len(session_lengths))
    raw_corpus_file.close()
    result.close()


def subtitle_process_pipeline():
    print("subtitle_process_pipeline")
    raw_corpus_file_name = Config.raw_subtitle_corpus_path
    result_file_name = os.path.join(Config.clean_chat_corpus_root, "subtitle.tsv")
    prepocess(raw_corpus_file_name, result_file_name)
