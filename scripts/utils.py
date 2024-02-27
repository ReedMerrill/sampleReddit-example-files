"""Utilities for logging and data processing."""

import time
import re
import string
import emojis
from langdetect import detect_langs, LangDetectException
import pandas as pd


def process_user_ids(id_list):
    """Clean the user IDs obtained during runs of sample.py.

    Inputs: list of user IDs
    Returns: Cleaned list of user IDs
        - removes duplicates
        - removes AutoModerator
        - removes None values
    """
    no_dupes = list(set(id_list))

    return [user for user in no_dupes if user not in ("None", "AutoModerator")]


def log_to_file(path, message):
    """output logging events to a file"""
    with open(f"{path}", "a") as file:
        file.write(message)


def estimate_time_remaining(task_index, total_tasks, start_time):
    """Estimate the time remaining.
    - Calculates the time per task for all tasks complete so far.
    - Outputs the time per task multiplied by the number of remaining
    tasks.
    """
    elapsed = (time.time() - start_time) / 3600  # convert seconds to hours
    t_per_task = elapsed / (task_index + 1)
    estimate = t_per_task * (total_tasks - task_index)

    return estimate


def remove_urls(comment):
    """Coerce all data to string and remove URLs"""

    pattern = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
    word_list = str(comment).split()
    clean_word_list = [re.sub(pattern, "", word) for word in word_list]
    clean = " ".join(clean_word_list)

    return clean


def remove_emojis(comment):
    """Removes emojis from a list of strings."""

    string = str(comment)
    decoded = emojis.decode(string)
    word_list = decoded.split()
    clean_word_list = [re.sub(r":\w+:", "", word) for word in word_list]
    clean = " ".join(clean_word_list)

    return clean


def check_language(comment):
    """Check that strings are English and return them if they are, or NA if
    not."""

    # Catch some basic problematic cases

    # remove punctuation
    translator = str.maketrans("", "", string.punctuation)
    _comment = comment.translate(translator)

    # if the comment is empty, return NA
    if len(str(_comment).split()) == 0:
        return pd.NA

    # assume that single word comments are in English
    # if its one word it can often be misclassified
    if len(str(_comment).split()) == 1:
        return comment

    # speed things up by only classifying based on the first 20 words
    if len(str(_comment).split()) > 20:
        _comment = " ".join(str(_comment).split()[:20])

    # check the language
    # if detect_langs throws an error, return NA
    try:
        langs_raw = detect_langs(_comment)

        langs = str(langs_raw[0]).split(":")[0]
        probs = str(langs_raw[0]).split(":")[1]
        langs_dict = {langs: float(probs)}

        highest_prob = max(langs_dict.values())

        if "en" in langs_dict.keys() and langs_dict["en"] == highest_prob:
            return comment
        else:
            return pd.NA

    except Exception as e:
        print(e)
        return pd.NA
