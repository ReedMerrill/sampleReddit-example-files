"""A job script that samples Reddit users from an initial list of subreddits.
"""

import json
import time
import datetime
from snow_roll import sample


# single sub for testing
PROJECT_PATH = "/home/reed/Projects/learned-toxicity-reddit/reddit-api/"
INPUT_PATH = f"{PROJECT_PATH}data/seed-subreddits.json"
OUTPUT_PATH = f"{PROJECT_PATH}data/"

# log file setup
LOG_DESC = "sample"
LOG_FILE_PATH = f"{PROJECT_PATH}logs/{LOG_DESC}_{datetime.datetime.now()}.txt"

# load sample seeds
SEEDS_DICT = json.load(open(INPUT_PATH))
SEED_SUBREDDITS = SEEDS_DICT["all"]


def main():
    """Iterate through the sampling structure, saving the elements used in
    sampling at each level.
    """

    start = time.time()

    print("Initializing API Instance")

    reddit = sample.setup_access()

    print("Initialization complete.")

    # set subreddit sample parameters
    time_period = "year"
    n_submissions = 3

    # initialize sample logging dicts
    seed_to_posts = {}
    post_to_comments = {}
    comment_to_user = {}
    users = {"users": []}

    # initialize output CSV
    with open(OUTPUT_PATH + "user-sample.csv", "a") as outfile:
        outfile.write("users\n")

    # iterate through the seed subreddits, getting a list of top posts IDs
    for seed in SEED_SUBREDDITS:

        posts = sample.get_top_posts(
            reddit=reddit,
            subreddit_name=seed,
            time_period=time_period,
            n_submissions=n_submissions,
        )

        # add a key "seed" with the post IDs as items
        seed_to_posts.update({seed: posts})

        # iterate through the posts, retreiving their comment IDs
        for i, post in enumerate(posts):

            comments = sample.get_post_comments_ids(reddit=reddit, submission_id=post)

            post_to_comments.update({post: comments})

            # iterate through the comments, retreiving each one's author
            for comment in comments:

                user = sample.get_comment_author(reddit=reddit, comment_id=comment)

                # add the new comment/user pair to the dict
                comment_to_user.update({comment: user})

                # append user/comment pair to dict
                users["users"].append(user)

                # append the values to the output CSV
                with open(OUTPUT_PATH + "user-sample.csv", "a") as outfile:
                    outfile.write(f"{user}\n")

                time.sleep(0.5)

            log_string = (
                f'{datetime.datetime.now()} - Finished Post {i + 1} of seed "{seed}"\n'
            )
            # logging to file
            with open(LOG_FILE_PATH, "a") as log_file:
                log_file.write(log_string)

    output_dict = {
        "seed_to_posts": seed_to_posts,
        "post_to_comments": post_to_comments,
        "comment_to_user": comment_to_user,
    }

    # output sampling procedure
    with open(OUTPUT_PATH + "sampling-prodecure.json", "w") as outfile:
        json.dump(output_dict, outfile, indent=4)

    finished = time.time()

    job_time = (finished - start) / 60

    print(f"Job took {job_time} minutes")


if __name__ == "__main__":
    main()
