from textwrap import dedent
from argparse import Namespace

import pytest

import task_Margasov_Arsenii_inverted_index

# DATASET_BIG_FILEPATH = "./wikipedia_sample"
DATASET_SMALL_FILEPATH = "./small_wikipedia_sample"
DATASET_TINY_FILEPATH = "./tiny_wikipedia_sample"
SMALL_INVERTED_INDEX_STORE_PATH = "./small_inverted.index"


# @pytest.mark.skip
@pytest.mark.parametrize(
    "dataset_filepath",
    [
        # pytest.param(DATASET_BIG_FILEPATH, marks=[pytest.mark.skip]),
        DATASET_SMALL_FILEPATH,
        DATASET_TINY_FILEPATH,
    ],
)
def test_process_build_can_load_documents(dataset_filepath):
    task_Margasov_Arsenii_inverted_index.process_build(dataset_filepath, "inverted.index")


def test_process_queries_can_process_all_queries_from_correct_file(capsys):
    with open("queries.txt") as queries_fin:
        task_Margasov_Arsenii_inverted_index.process_queries(
            inverted_index_filepath=SMALL_INVERTED_INDEX_STORE_PATH,
            query_file=queries_fin,
        )
        captured = capsys.readouterr()
        assert "load inverted index" not in captured.out
        assert "load inverted index" in captured.err
        assert "123" in captured.out
        assert "lol" not in captured.err


def test_process_queries_from_stdin_can_process_all_queries_from_stdin(capsys):
    task_Margasov_Arsenii_inverted_index.process_queries_from_stdin(
        inverted_index_filepath=SMALL_INVERTED_INDEX_STORE_PATH,
        query_without_file=[["in"]],
    )
    captured = capsys.readouterr()
    assert "load inverted index" not in captured.out
    assert "load inverted index" in captured.err
    assert "123,6" in captured.out
    assert "lol" not in captured.err


def test_can_load_documents_v1():
    # dataset example:
    # 123 some words A_word and nothing
    # 2   some word B_word in this dataset
    # 5   famous_phrases to be or not to be
    # 37  all words such as A_word and B_word are here
    documents = task_Margasov_Arsenii_inverted_index.load_documents(DATASET_TINY_FILEPATH)
    etalon_documents = {
        "123": "some words A_word and nothing",
        "2": "some word B_word in this dataset",
        "5": "famous_phrases to be or not to be",
        "37": "all words such as A_word and B_word are here"
    }
    assert etalon_documents == documents, (
        "load_documents incorrectly loaded dataset"
    )


def test_can_load_documents_v2(tmpdir):
    dataset_str = dedent("""\
        123	some words A_word and nothing
        2	some word B_word in this dataset
        5	famous_phrases to be or not to be
        37	all words such as A_word and B_word are here
    """)
    dataset_fio = tmpdir.join("tiny.dataset")
    dataset_fio.write(dataset_str)
    documents = task_Margasov_Arsenii_inverted_index.load_documents(dataset_fio)
    etalon_documents = {
        "123": "some words A_word and nothing",
        "2": "some word B_word in this dataset",
        "5": "famous_phrases to be or not to be",
        "37": "all words such as A_word and B_word are here"
    }
    assert etalon_documents == documents, (
        "load_documents incorrectly loaded dataset"
    )


DATASET_TINY_STR = dedent("""\
        123	some words A_word and nothing
        2	some word B_word in this dataset
        5	famous_phrases to be or not to be
        37	all words such as A_word and B_word are here
    """)


@pytest.fixture
def tiny_dataset_fio(tmpdir):
    dataset_fio = tmpdir.join("dataset.txt")
    dataset_fio.write(DATASET_TINY_STR)
    return dataset_fio


def test_can_load_documents(tiny_dataset_fio):
    documents = task_Margasov_Arsenii_inverted_index.load_documents(tiny_dataset_fio)
    etalon_documents = {
        "123": "some words A_word and nothing",
        "2": "some word B_word in this dataset",
        "5": "famous_phrases to be or not to be",
        "37": "all words such as A_word and B_word are here"
    }
    assert etalon_documents == documents, (
        "load_documents incorrectly loaded dataset"
    )


@pytest.mark.parametrize(
    "query, etalon_answer",
    [
      pytest.param(["A_word"], [123, 37]),
      pytest.param(["B_word"], [2, 37], id="B_word"),
      pytest.param(["A_word", "B_word"], [37], id="both words"),
      pytest.param(["word_does_not_exist"], [], id="word does not exist"),
    ],
)
def test_query_inverted_index_intersect_results(tiny_dataset_fio, query, etalon_answer):
    documents = task_Margasov_Arsenii_inverted_index.load_documents(tiny_dataset_fio)
    tiny_inverted_index = task_Margasov_Arsenii_inverted_index.build_inverted_index(documents)
    answer = tiny_inverted_index.query(query)
    assert sorted(answer) == sorted(etalon_answer), (
        f"Expected answer id {etalon_answer}, but you got {answer}"
    )


def test_can_load_wikipedia_sample():
    documents = task_Margasov_Arsenii_inverted_index.load_documents(DATASET_TINY_FILEPATH)
    assert len(documents) == 4, (
        "you incorrectly loaded Wikipedia sample"
    )


@pytest.fixture
def wikipedia_documents():
    documents = task_Margasov_Arsenii_inverted_index.load_documents(DATASET_TINY_FILEPATH)
    return documents


@pytest.fixture
def small_sample_wikipedia_documents():
    documents = task_Margasov_Arsenii_inverted_index.load_documents(DATASET_SMALL_FILEPATH)
    return documents


def test_can_build_and_query_inverted_index(small_sample_wikipedia_documents):
    wikipedia_inverted_index = task_Margasov_Arsenii_inverted_index.build_inverted_index(small_sample_wikipedia_documents)
    doc_ids = wikipedia_inverted_index.query(["wikipedia"])
    assert isinstance(doc_ids, list), "inverted index query should return list"


@pytest.fixture
def wikipedia_inverted_index(wikipedia_documents):
    wikipedia_inverted_index = task_Margasov_Arsenii_inverted_index.build_inverted_index(wikipedia_documents)
    return wikipedia_inverted_index


@pytest.fixture
def small_wikipedia_inverted_index(small_sample_wikipedia_documents):
    wikipedia_inverted_index = task_Margasov_Arsenii_inverted_index.build_inverted_index(small_sample_wikipedia_documents)
    return wikipedia_inverted_index


def test_can_dump_and_load_inverted_index(tmpdir, small_wikipedia_inverted_index):
    index_fio = tmpdir.join("index.dump")
    small_wikipedia_inverted_index.dump(index_fio)
    loaded_inverted_index = task_Margasov_Arsenii_inverted_index.InvertedIndex.load(index_fio)
    # print(f"loaded: {loaded_inverted_index}")
    # print(f"current: {small_wikipedia_inverted_index}")
    assert small_wikipedia_inverted_index == loaded_inverted_index, (
        "load should return the same inverted index"
    )
