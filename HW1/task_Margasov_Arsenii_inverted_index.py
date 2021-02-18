#!/usr/bin/env python3
"""InvertedIndex implemented here"""
from struct import pack, unpack, calcsize
import sys
from io import TextIOWrapper
# import re
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, \
    FileType, ArgumentTypeError

from collections import defaultdict

# RE_SPLIT_PATTERN = r"\s+"
DEFAULT_DATASET_PATH = "./resources/tiny_wikipedia_sample"
DEFAULT_INVERTED_INDEX_STORE_PATH = "inverted.index"


class StoragePolicy:
    """
    Main StoragePolicy
    """

    @staticmethod
    def dump(word_to_docs_mapping, filepath: str):
        """

        Args:
            word_to_docs_mapping: internal mapping of InvertedIndex
            filepath: path to dump

        Returns:
            None
        """

    @staticmethod
    def load(filepath: str):
        """

        Args:
            filepath: path to load InvertedIndex

        Returns:
            None
        """


# class JsonStoragePolicy(StoragePolicy):
#     """
#     JsonStoragePolicy
#     """
#
#     @staticmethod
#     def dump(word_to_docs_mapping: defaultdict, filepath: str):
#         """
#
#         Args:
#             word_to_docs_mapping: internal mapping of InvertedIndex
#             filepath: path to dump
#
#         Returns:
#             None
#         """
#         print(f"dump inverted index to {filepath}", file=sys.stderr)
#         with open(filepath, 'w') as file:
#             term_doc_id_to_dump = {idx: list(ids) for idx, ids in word_to_docs_mapping.items()}
#             json.dump(term_doc_id_to_dump, file)
#
#     @staticmethod
#     def load(filepath: str) -> 'InvertedIndex':
#         """
#
#         Args:
#             filepath: path to load InvertedIndex
#
#         Returns:
#             InvertedIndex
#         """
#         print(f"load inverted index from filepath {filepath}", file=sys.stderr)
#         with open(filepath, "r") as file:
#             return InvertedIndex(term_doc_id=json.load(file))


class BinaryStoragePolicy(StoragePolicy):
    """
    BinaryStoragePolicy with struct
    """

    # DEFAULT_NUM_BINARY_ENCODING = "i"
    # UNS_SHORT_NUM_BINARY_ENCODING = "H"
    @staticmethod
    def dump(word_to_docs_mapping: defaultdict, filepath: str):
        """

        Args:
            word_to_docs_mapping: internal mapping of InvertedIndex
            filepath: path to dump

        Returns:
            None
        """
        print(f"dump inverted index to {filepath}", file=sys.stderr)
        with open(filepath, 'wb') as file:
            length_of_dict = len(word_to_docs_mapping)
            file.write(pack(">i", length_of_dict))
            for term, ids in word_to_docs_mapping.items():
                term_encoded = term.encode()
                length_of_term = len(term_encoded)
                file.write(pack(">H", length_of_term))
                file.write(pack(">" + str(length_of_term) + "s", term_encoded))
                file.write(pack(">H", len(ids)))
                file.write(pack(">" + str(len(ids)) + "H", *list(ids)))

    @staticmethod
    def load(filepath: str):
        """

        Args:
            filepath: path to load InvertedIndex

        Returns:
            InvertedIndex
        """
        print(f"load inverted index from filepath {filepath}", file=sys.stderr)
        term_doc_id = defaultdict(set)
        with open(filepath, 'rb') as file:
            length_of_dict = file.read(calcsize(">i"))
            length_of_dict = unpack(">i", length_of_dict)[0]
            for _ in range(length_of_dict):
                length_of_term = file.read(calcsize(">H"))
                length_of_term = unpack(">H", length_of_term)[0]
                term = unpack(">" + str(length_of_term) + "s",
                              file.read(length_of_term))[0]
                term = term.decode()
                length_of_ids = file.read(calcsize(">H"))
                length_of_ids = unpack(">H", length_of_ids)[0]
                ids = file.read(length_of_ids * calcsize(">H"))
                ids = set(unpack(">" + str(length_of_ids) + "H", ids))
                term_doc_id[term] = ids

            return InvertedIndex(term_doc_id=term_doc_id)


class EncodedFileType(FileType):
    """
    Added encoding for FileType
    """

    def __call__(self, string):
        # the special argument "-" means sys.std{in,out}
        if string == '-':
            if 'r' in self._mode:
                stdin = TextIOWrapper(sys.stdin.buffer, encoding=self._encoding)
                return stdin
            elif 'w' in self._mode:
                stdout = TextIOWrapper(sys.stdout.buffer, encoding=self._encoding)
                return stdout
            else:
                msg = 'argument "-" with mode %r' % self._mode
                raise ValueError(msg)

        # all other arguments are used as file names
        try:
            return open(string, self._mode, self._bufsize, self._encoding,
                        self._errors)
        except OSError as e:
            args = {'filename': string, 'error': e}
            message = "can't open '%(filename)s': %(error)s"
            raise ArgumentTypeError(message % args)


class InvertedIndex:
    """InvertedIndex: term -> document_ids"""

    def __init__(self, term_doc_id: defaultdict):
        self.term_doc_id = term_doc_id

    def __eq__(self, other):
        outcome = (
            (self.term_doc_id.keys() == other.term_doc_id.keys())
            and
            (list(map(list,
                  other.term_doc_id.values())) == list(map(list,
                                                           self.term_doc_id.values())))
        )
        return outcome

    def __repr__(self):
        return str(self.term_doc_id)

    def query(self, words: list) -> list:
        """

        Args:
            words: list of words to query

        Returns:
            list of ids of documents
        """
        assert isinstance(words, list), (
            "query should be provided with a list of words, but provided: "
            f"{repr(words)}"
        )
        print(f"query inverted index with request {repr(words)}", file=sys.stderr)
        words_set = set(words)
        intersected_words = words_set & set(self.term_doc_id.keys())
        if len(words_set) != len(intersected_words):
            return []
        relevant_ids = set()
        for term in intersected_words:
            if len(relevant_ids) == 0:
                relevant_ids = relevant_ids | set(self.term_doc_id[term])
            else:
                relevant_ids = relevant_ids & set(self.term_doc_id[term])
        return list(relevant_ids)

    def dump(self, filepath: str):
        """

        Args:
            filepath: filepath to dump inverted index

        Returns:
            None
        """
        # JsonStoragePolicy.dump(self.term_doc_id, filepath)
        BinaryStoragePolicy.dump(self.term_doc_id, filepath)

    @classmethod
    def load(cls, filepath: str) -> 'InvertedIndex':
        """

        Args:
            filepath: filepath to load InvertedIndex

        Returns:
            InvertedIndex
        """
        # return JsonStoragePolicy.load(filepath)
        return BinaryStoragePolicy.load(filepath)


def load_documents(filepath: str) -> dict:
    """

    Args:
        filepath: path to file with documents to load

    Returns:
        dict of documents where key=id_of_document, val=text_of_document
    """
    print(f"loading documents from {filepath}", file=sys.stderr)
    documents = {}
    with open(filepath, "r") as file:
        for line in file:
            idx, text = line.strip().split('\t', 1)
            documents[idx] = text
    return documents


# def load_queries(file: TextIOWrapper) -> list:
#     """
#
#     Args:
#         file: path to queries
#
#     Returns:
#         list with queries
#     """
#     queries = []
#     for line in file:
#         queries.append(line.strip().split())
#     return queries


def build_inverted_index(documents: dict) -> InvertedIndex:
    """

    Args:
        documents: dict of documents for building InvertedIndex

    Returns:
        InvertedIndex
    """
    term_doc_id = defaultdict(set)
    for idx, text in documents.items():
        # text = re.split(RE_SPLIT_PATTERN, text)
        text = text.split()
        for word in text:
            term_doc_id[word].add(int(idx))
    print("InvertedIndex created", file=sys.stderr)
    return InvertedIndex(term_doc_id=term_doc_id)


def callback_build(arguments):
    """

    Args:
        arguments: arguments from parser

    Returns:
        process_build(dataset, inverted_index_filepath)
    """
    return process_build(arguments.dataset_filepath, arguments.inverted_index_filepath)


def process_build(dataset_filepath, inverted_index_filepath):
    """

    Args:
        dataset_filepath: filepath to dataset for processing build
        inverted_index_filepath: filepath for built inverted index

    Returns:

    """
    print(f"build from {dataset_filepath}, to {inverted_index_filepath}", file=sys.stderr)
    documents = load_documents(dataset_filepath)
    inverted_index = build_inverted_index(documents)
    inverted_index.dump(inverted_index_filepath)


def callback_query(arguments):
    """

    Args:
        arguments (Namespace): arguments from parser

    Returns:
        if query from stdin:
            process_queries_from_stdin(inverted_index_filepath, query_without_file)
        else:
            process_queries(inverted_index_filepath, query_file)
    """
    if arguments.query_without_file is not None:
        process_queries_from_stdin(arguments.inverted_index_filepath,
                                   arguments.query_without_file)
    else:
        process_queries(arguments.inverted_index_filepath,
                        arguments.query_file)


def process_queries_from_stdin(inverted_index_filepath, query_without_file):
    """

    Args:
        inverted_index_filepath: filepath to InvertedIndex
        query_without_file: list of lists of queries

    Returns:
        None
    """
    print(f"read queries from {query_without_file}", file=sys.stderr)
    inverted_index = InvertedIndex.load(inverted_index_filepath)
    relevant_ids = []
    for query in query_without_file:
        tmp_relevant_ids: list = inverted_index.query(query)
        tmp_relevant_ids = list(map(str, tmp_relevant_ids))
        relevant_ids.append(','.join(tmp_relevant_ids))
    # print(len(relevant_ids[0].split(',')))
    sys.stdout.buffer.write(('\n'.join(relevant_ids)).encode())


def process_queries(inverted_index_filepath, query_file):
    """

    Args:
        inverted_index_filepath: filepath to InvertedIndex
        query_file: TextIOWrapper file

    Returns:
        None
    """
    print(f"read queries from {query_file}", file=sys.stderr)
    inverted_index = InvertedIndex.load(inverted_index_filepath)
    # for line in query_file:
    #     queries.append(line.strip().split())
    # queries = load_queries(query_file)
    relevant_ids = []
    for line in query_file:
        # if not isinstance(line, str):
        #     continue
        if line is None:
            continue
        query = list(map(str, line.strip().split()))
        tmp_relevant_ids = inverted_index.query(query)
        tmp_relevant_ids = list(map(str, tmp_relevant_ids))
        relevant_ids.append(','.join(tmp_relevant_ids))
    sys.stdout.buffer.write(('\n'.join(relevant_ids)).encode())


def setup_parser(parser):
    """

    Args:
        parser: ArgumentParser from argparse

    Returns:
        ArgumentParser with adjusted arguments
    """
    subparsers = parser.add_subparsers(help="choose command")

    build_parser = subparsers.add_parser(
        "build",
        help="build inverted index and save in binary format into hard drive",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    build_parser.add_argument(
        "-d", "--dataset", dest="dataset_filepath",
        default=DEFAULT_DATASET_PATH,
        help="path to dataset to load, default path is %(default)s",
    )
    build_parser.add_argument(
        "-o", "--output", dest="inverted_index_filepath",
        default=DEFAULT_INVERTED_INDEX_STORE_PATH,
        help="path to store inverted index in a binary format, \
             default path is %(default)s",
    )
    build_parser.set_defaults(callback=callback_build)

    query_parser = subparsers.add_parser(
        "query", help="query inverted index",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    query_parser.add_argument(
        "-i ", "--index", default=DEFAULT_INVERTED_INDEX_STORE_PATH,
        dest="inverted_index_filepath",
        help="path to read inverted index in a binary format,\
         default path is %(default)s",
    )

    query_file_group = query_parser.add_mutually_exclusive_group(required=False)
    query_file_group.add_argument(
        "--query-file-utf8", dest="query_file",
        type=EncodedFileType("r", encoding="utf-8"),
        default=TextIOWrapper(sys.stdin.buffer, encoding="utf-8"),
        help="query file to get queries for inverted index",
    )
    query_file_group.add_argument(
        "--query-file-cp1251", dest="query_file",
        type=EncodedFileType("r", encoding="cp1251"),
        default=TextIOWrapper(sys.stdin.buffer, encoding="cp1251"),
        help="query file to get queries for inverted index",
    )
    query_parser.add_argument(
        "--query",
        nargs="+",
        action="append",
        dest="query_without_file",
    )
    query_parser.set_defaults(callback=callback_query)


# noinspection PyTypeChecker
def main():
    """Used from CLI"""
    parser = ArgumentParser(
        prog="inverted-index",
        description="tool to build, dump, load and query inverted index",
        formatter_class=ArgumentDefaultsHelpFormatter,

    )
    setup_parser(parser)
    arguments = parser.parse_args()
    arguments.callback(arguments)
    # idx = build_inverted_index(load_documents("./resources/wikipedia_sample"))
    # JsonStoragePolicy.dump(idx.term_doc_id, "./wiki_dump.json")


if __name__ == "__main__":
    main()
