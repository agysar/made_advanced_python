#!/usr/bin/env python3
"""Stackoverflow analytics implemented here"""
import csv
import logging
import logging.config
import re
import json
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

from collections import defaultdict

import yaml
import lxml.etree as et

RE_SPLIT_PATTERN = r"\w+"
APPLICATION_NAME = "stackoverflow_analytics"
DEFAULT_LOGGING_CONFIG_FILEPATH = "logging.conf.yml"
DEFAULT_QUESTIONS_PATH = "./questions.xml"
DEFAULT_STOP_WORDS_PATH = "./stop_words.txt"
DEFAULT_QUERIES_PATH = "./queries.csv"

logger = logging.getLogger(APPLICATION_NAME)


def load_documents(filepath: str, encoding: str = "utf-8") -> list:
    """

    Args:
        filepath: path to file with documents to load
        encoding: file encoding
    Returns:
        list of documents
    """
    documents = []
    with open(filepath, "r", encoding=encoding) as file:
        if filepath.endswith(".csv"):
            file = csv.reader(file, delimiter=',')
        for line in file:
            if encoding == "koi8-r":
                line = line.strip("\n")
            documents.append(line)
    return documents


def process_arguments(quest_fp, stopwords_fp, queries_fp):
    """

    Args:
        quest_fp: filepath to questions
        stopwords_fp: filepath to stopwords
        queries_fp: filepath to queries

    Returns:
        dict of words and their scores
    """
    questions = load_documents(quest_fp)
    stop_words = set(load_documents(stopwords_fp, encoding="koi8-r"))
    queries = load_documents(queries_fp)
    xml_lines = []
    for quest in questions:
        root = et.fromstring(quest)
        try:
            post_id = int(root.xpath('@PostTypeId')[0])
            if post_id != 1:
                continue
            score = int(root.xpath('@Score')[0])
            date = int(root.xpath('@CreationDate')[0][:4])
            words = re.findall(RE_SPLIT_PATTERN, root.xpath('@Title')[0].lower())
            if len(words) == 0:
                continue
            words = [word for word in words if word not in stop_words]
            xml_dict = {'words': words, 'date': date, 'score': score}
            xml_lines.append(xml_dict)
        except IndexError:
            continue
    logger.info("process XML dataset, ready to serve queries")
    for query in queries:
        words_scores = defaultdict(int)
        from_date, to_date, amount = map(int, query)
        logger.debug('got query "%s,%s,%s"', from_date, to_date, amount)
        for xml_line in xml_lines:
            if from_date <= xml_line['date'] <= to_date:
                unique_words = set()
                for word in xml_line['words']:
                    if word not in unique_words:
                        words_scores[word] += xml_line['score']
                        unique_words.add(word)
        words_scores = list(sorted(words_scores.items(), key=lambda kv: (-kv[1], kv[0])))[:amount]
        if len(words_scores) < amount:
            logger.warning('not enough data to answer, found %s words out of %s for period "%s,%s"', len(words_scores), amount, from_date, to_date)
        out = {'start': from_date, 'end': to_date, 'top': words_scores}
        print(json.dumps(out))
    logger.info("finish processing queries")


def callback_arguments(arguments):
    """

    Args:
        arguments: from parser

    Returns:
        None
    """
    process_arguments(arguments.questions_filepath,
                      arguments.stopwords_filepath,
                      arguments.queries_filepath)


def setup_parser(parser):
    """

    Args:
        parser: ArgumentParser from argparse

    Returns:
        ArgumentParser with adjusted arguments
    """
    parser.add_argument(
        "--questions", default=DEFAULT_QUESTIONS_PATH,
        dest="questions_filepath",
        help="path to read questions,\
         default path is %(default)s",
    )
    parser.add_argument(
        "--stop-words", default=DEFAULT_STOP_WORDS_PATH,
        dest="stopwords_filepath",
        help="path to read stop words,\
         default path is %(default)s",
    )
    parser.add_argument(
        "--queries", default=DEFAULT_QUERIES_PATH,
        dest="queries_filepath",
        help="path to read queries,\
         default path is %(default)s",
    )
    parser.set_defaults(callback=callback_arguments)


def setup_logging():
    """
        Setup logging
    Returns:
        None
    """
    with open(DEFAULT_LOGGING_CONFIG_FILEPATH) as config_fin:
        logging.config.dictConfig(yaml.safe_load(config_fin))


def main():
    """Used from CLI"""
    setup_logging()
    parser = ArgumentParser(
        prog="stackoverflow-analytics",
        description="tool for analyzing stackoverflow",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    setup_parser(parser)
    arguments = parser.parse_args()
    arguments.callback(arguments)


if __name__ == "__main__":
    main()
