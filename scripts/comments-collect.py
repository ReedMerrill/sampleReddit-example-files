"""Takes 'user-sample.csv' as input and collects each user's recent comments.
"""

import time
from datetime import datetime
import pandas as pd
import sample
import utils

PROJECT_PATH = "/home/reed/Projects/learned-toxicity-reddit/reddit-api/"
INPUT_PATH = f"{PROJECT_PATH}data/sample/user-sample-subset-20pct.csv"
OUTPUT_PATH = f"{PROJECT_PATH}data/comments/20pct-users-subset_comments.csv"
COMMENT_LIMIT = 1000
LOG_PATH = f"{PROJECT_PATH}/logs/user-comment-extraction_{datetime.now()}.txt"


def main():
    """Gets an API instance, cleans the usernames, fetches all comments for
    each user, then fetches each comment's metadata.
    """
    # initialize log file
    start_time = time.time()
    utils.log_to_file(LOG_PATH, f"{datetime.now()} - Begin Fetching comments...\n")
    # setup a PRAW reddit instance
    reddit = sample.setup_access()
    print("API Authentication Successful")
    # read in users subset
    users = pd.read_csv(INPUT_PATH)
    users_list = list(users["users"])
    # iterate over list of user, extracting each user's comment metadata
    for i, user in enumerate(users_list):
        # initialize dict to store a single user's comments
        # make comment metadata dict using reddit API
        sample.get_user_comments(
            reddit=reddit,
            user_id=user,
            limit=COMMENT_LIMIT,
            out_file_path=OUTPUT_PATH,
            log_path=LOG_PATH,
        )
        estimate = utils.estimate_time_remaining(
            task_index=i, total_tasks=len(users_list), start_time=start_time
        )
        print(f"Finished collecing data for User {i + 1}")
        print(f"Time remaining: ~{estimate} hours")
    # final logging
    total_time_hours = (time.time() - start_time) / 3600
    print(f"Total Time Elapsed: {total_time_hours}")
    utils.log_to_file(LOG_PATH, f"Total Time Elapsed: {total_time_hours}")


if __name__ == "__main__":

    main()
