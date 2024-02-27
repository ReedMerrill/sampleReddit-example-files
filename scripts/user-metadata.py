"""Takes 'user-sample.csv' as input and collects each user's recent comments.
"""

import time
from datetime import datetime
import pandas as pd
import sample
import utils

PROJECT_PATH = "/home/reed/Projects/learned-toxicity-reddit/reddit-api"
INPUT_PATH = f"{PROJECT_PATH}/data/sample/user-sample.csv"
OUTPUT_PATH = f"{PROJECT_PATH}/data/sample/user-metadata.csv"
log_path = f"{PROJECT_PATH}/logs/user-metadata_{datetime.now()}.txt"

# read users list and remove duplicates and moderators
users = pd.read_csv(INPUT_PATH)
users_list = utils.process_user_ids(list(users["users"]))


def main():
    """Gets an API instance, cleans the usernames, fetches all comments for
    each user, then fetches each comment's metadata.
    """
    # initialize log file
    start_time = time.time()
    utils.log_to_file(log_path, f"{datetime.now()} - Begin Fetching user metadata...\n")
    # setup a PRAW reddit instance
    reddit = sample.setup_access()
    print("API Authentication Successful")
    # iterate over list of user, extracting each user's comment metadata
    for i, user in enumerate(users_list):
        # make comment metadata dict using reddit API
        sample.get_user_metadata(
            reddit=reddit,
            user_id=user,
            out_file_path=OUTPUT_PATH,
            log_path=log_path,
            n_retries=3,
        )
        # log finish time estimate
        estimate = utils.estimate_time_remaining(
            task_index=i, total_tasks=len(users_list), start_time=start_time
        )
        utils.log_to_file(log_path, f"Finished collecing data for User {i + 1}\n")
        utils.log_to_file(log_path, f"Time remaining: ~{estimate} hours\n")
    # final logging
    total_time = (time.time() - start_time) / 3600
    utils.log_to_file(log_path, f"Total Time Elapsed: {total_time}\n")


if __name__ == "__main__":
    main()
