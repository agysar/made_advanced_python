from argparse import Namespace

import pytest

import task_Margasov_Arsenii_stackoverflow_analytics

DEFAULT_QUESTIONS_PATH = "./questions.xml"
DEFAULT_STOP_WORDS_PATH = "./stop_words.txt"
DEFAULT_QUERIES_PATH = "./queries.csv"


@pytest.mark.parametrize(
    "filepath, encoding",
    [
        (DEFAULT_QUESTIONS_PATH, "utf-8"),
        (DEFAULT_STOP_WORDS_PATH, "koi8-r"),
        (DEFAULT_QUERIES_PATH, "utf-8"),
    ],
)
def test_process_can_load_documents(filepath, encoding):
    task_Margasov_Arsenii_stackoverflow_analytics.load_documents(filepath, encoding)


def test_process_arguments_correct_output(capsys):
    task_Margasov_Arsenii_stackoverflow_analytics.process_arguments(DEFAULT_QUESTIONS_PATH,
                                                                    DEFAULT_STOP_WORDS_PATH,
                                                                    DEFAULT_QUERIES_PATH)
    captured = capsys.readouterr()
    assert "{}" not in captured.out
    assert '{"start": 2019, "end": 2019, "top": [["seo", 15], ["better", 10]]}' in captured.out
    assert '{"start": 2019, "end": 2020, "top": [["better", 30], ["javascript", 20], ["python", 20], ["seo", 15]]}' in captured.out


