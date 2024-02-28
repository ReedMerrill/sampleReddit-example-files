"""
A collection of wrappers for PRAW to aid in random sampling of Reddit data.
Includes utilities for sampling all major Reddit entities:
    - Subreddits
    - Posts
    - Comments
    - Users
"""

# for sampling
import time
import os
import re
import string
import emojis
import praw
from datetime import datetime
import pandas as pd
from langdetect import detect_langs
from prawcore.exceptions import TooManyRequests


def setup_access(client_id, client_secret, password, user_agent, username):
    """Create an instance for API access"""

    print("API Instance initialized.")

    instance = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        password=password,
        user_agent=user_agent,
        username=username,
    )
    return instance


def get_posts_list(
    api_instance, subreddit_name, post_filter: str, time_period, n_posts
):
    """Takes the name of a subreddit, a time period, and the desired number of submissions
    and returns a list of the URLs of that subreddit's top posts.

    nb this is a separate function because doing so retains a list of the submissions, and parsing
    each level of the user extraction process into its own function avoids messy function outputs
    and is more efficient.
    Returns: list of top post URLs
    """
    # conditions for the filter type
    if post_filter == "top":
        # create the generator
        submission_generator = api_instance.subreddit(subreddit_name).top(
            time_filter=time_period, limit=n_posts
        )
        # return generator outputs as a list
        return [str(submission.id) for submission in submission_generator]

    elif post_filter == "new":
        submission_generator = api_instance.subreddit(subreddit_name).new(
            time_filter=time_period, limit=n_posts
        )
        return [str(submission.id) for submission in submission_generator]

    elif post_filter == "hot":
        submission_generator = api_instance.subreddit(subreddit_name).hot(
            time_filter=time_period, limit=n_posts
        )
        return [str(submission.id) for submission in submission_generator]


def get_post_comments_ids(reddit, submission_id):
    """Takes a submission ID and returns a (flattened) list of all its comments."""
    # create a post instance
    comments = reddit.submission(submission_id).comments
    # replace_more() updates the comment forest by resolving instances of MoreComments
    comments.replace_more()
    # list() flattens the comment forest to a simple list of all comments on the submission
    return [comment.id for comment in comments.list()]


def get_comment_author(reddit, comment_id):
    """Takes a comment ID and return that comment's author."""
    return str(reddit.comment(comment_id).author)


def sample_reddit(
    api_instance, seed_subreddits, post_filter, time_period, n_posts, log_file_path
):
    """Generate a snowball sample from a list of subreddits. From the subreddits, get a
    list `n_submissions` number of of posts from `time_period`. From each post, get a list of
    comments and from each comment, get the author. Write the author to a CSV and return a
    dictionary that maps the subreddit to the posts, the posts to the comments, and the comments
    to the authors.

    Inputs:
    - api_instance: a PRAW instance
    - seed_subreddits: a list of subreddit names
    - time_period: a string representing the time period to sample from
    - n_submissions: the number of submissions to sample from each subreddit
    - output_path: the path to the output directory
    Returns: a dictionary with the following keys
    - subreddits_to_posts: a dictionary mapping subreddit names to lists of post IDs
    - posts_to_comments: a dictionary mapping post IDs to lists of comment IDs
    - comments_to_users: a dictionary mapping comment IDs to usernames
    - users: a dictionary with a single key "users" mapping to a list of usernames
    """
    start_time = time.time()

    # initialize sample logging dicts
    subreddits_to_posts = {}
    posts_to_comments = {}
    comments_to_users = {}
    users = {"users": []}

    # iterate through the seed subreddits, getting a list of top posts IDs
    for seed in seed_subreddits:

        posts = get_posts_list(
            api_instance=api_instance,
            subreddit_name=seed,
            post_filter=post_filter,
            time_period=time_period,
            n_posts=n_posts,
        )

        # add a key "seed" with the post IDs as items
        subreddits_to_posts.update({seed: posts})

        # iterate through the posts, retreiving their comment IDs
        for i, post in enumerate(posts):
            comments = get_post_comments_ids(reddit=api_instance, submission_id=post)
            posts_to_comments.update({post: comments})

            # iterate through the comments, retreiving each one's author
            for comment in comments:
                user = get_comment_author(reddit=api_instance, comment_id=comment)
                # add the new comment/user pair to the dict
                comments_to_users.update({comment: user})
                # append user/comment pair to dict
                users["users"].append(user)

                time.sleep(0.5)
            # logging
            log_string = f'{datetime.now()} - Finished Post {i + 1} of seed "{seed}"\n'
            log_to_file(f"{log_file_path}sample_log{datetime.now()}.txt", log_string)

    output_dict = {
        "subreddits_to_posts": subreddits_to_posts,
        "posts_to_comments": posts_to_comments,
        "comments_to_users": comments_to_users,
        "users": users["users"],
    }

    print(f"Sample complete. Time elapsed: {(time.time() - start_time) / 60} minutes")

    return output_dict, users


def get_user_comments(
    reddit,
    user_id,
    out_file_path,
    log_path,
    n_retries=3,
    limit=1000,
):
    """Takes a user ID and collects "limit" number (up to 1,000) of that
    user's most recent comments, with metadata. Filters "distinguished"
    comments, which are used to add a "MOD" decorator (used when engaging as a
    moderator rather than a community member). Writes data to disk one row at
    a time as a CSV.
    """

    # get a ListingGenerator for up to the user's 1,000 most recent comments
    user_comment_generator = reddit.redditor(user_id)

    col_names = [
        "comment_id",
        "username",
        "post_id",
        "subreddit_id",
        "timestamp",
        "parent_comment",  # if top-level, then returns the submission ID
        "upvotes",
        "text",
    ]

    # retry loop
    for i in range(n_retries):
        try:
            # iterate over the generator to call each comment by the user
            for comment in user_comment_generator.comments.new(limit=limit):
                # don't collect distinguished comments
                if comment.distinguished != "moderator":
                    # data to collect
                    comment_metadata = [
                        comment.id,
                        user_id,
                        comment.link_id,
                        comment.subreddit_id,
                        comment.created_utc,
                        comment.parent_id,
                        comment.score,
                        comment.body,
                    ]

                    data_row = pd.DataFrame([comment_metadata], columns=col_names)
                    # check if the file exists
                    file_exists = True if os.path.exists(out_file_path) else False
                    if file_exists is False:
                        with open(out_file_path, "w") as file:
                            data_row.to_csv(file, index=False, header=True)
                    else:
                        with open(out_file_path, "a") as file:
                            data_row.to_csv(file, index=False, header=False)
                # get another comment to account for skipping the mod comment
            # exit retry loop
            break

        # if a TooManyRequsts error is raised then the API rate limit has been exceeded.
        # Retry after sleeping. Sleep duration increases by a factor of 2 for 3 retries.
        except TooManyRequests as e:
            log_to_file(
                log_path, f"Error: {e} while fetching one of {user_id}'s comments\n"
            )
            print(f"Error: {e} while fetching user {user_id}")
            sleep_time = 1 * (2**i)  # each retry waits for longer: 1s, 2s, 4s
            print(f"Making {i + 1}st retry after waiting {sleep_time}s")
            time.sleep(sleep_time)

        # catch all other possible exceptions and break retry loop
        except Exception as e:
            log_to_file(
                log_path,
                f'Unresolved Error: "{e}" while fetching one of {user_id}\'s comments\n',
            )
            print(f'Error: "{e}" while fetching user {user_id}')
            break


def get_comments(
    api_instance,
    input_path,
    output_path,
    log_path,
    comment_limit=1000,
):
    """Gets an API instance, cleans the usernames, fetches all comments for
    each user, then fetches each comment's metadata.
    """
    # initialize log file
    start_time = time.time()
    log_to_file(log_path, f"{datetime.now()} - Begin Fetching comments...\n")
    # setup a PRAW reddit instance
    # read in users subset
    users = pd.read_csv(input_path)
    users_list = list(users["users"])
    # iterate over list of user, extracting each user's comment metadata
    for i, user in enumerate(users_list):
        # initialize dict to store a single user's comments
        # make comment metadata dict using reddit API
        get_user_comments(
            reddit=api_instance,
            user_id=user,
            limit=comment_limit,
            out_file_path=output_path,
            log_path=log_path,
        )
        estimate = estimate_time_remaining(
            task_index=i, total_tasks=len(users_list), start_time=start_time
        )
        print(f"Finished collecting comment data for user {i + 1}")
        print(f"Time remaining: ~{estimate} hours")
    # final logging
    total_time_hours = (time.time() - start_time) / 3600
    print(f"Total Time Elapsed: {total_time_hours}")
    log_to_file(log_path, f"Total Time Elapsed: {total_time_hours}")


def get_user_metadata(
    reddit,
    user_id,
    out_file_path,
    log_path,
    n_retries=3,
):
    """Iterates through a list of usernames and returns rows of data matching
    the metadata of each user.
    """
    # column names for csv output
    col_names = ["display_name", "id", "comment_karma", "total_karma", "created_utc"]
    # get a ListingGenerator for up to the user's 1,000 most recent comments
    user = reddit.redditor(user_id)

    # retry loop
    for i in range(n_retries):
        try:
            # iterate over the generator to call each comment by the user
            # get another comment to account for skipping the mod commen
            # data to collect
            metadata_list = [
                user_id,
                user.id,
                user.comment_karma,
                user.total_karma,
                user.created_utc,
            ]

            print(f'Finished collecting metadata for user "{user}"')
            # stream data to CSV file
            data_row = pd.DataFrame([metadata_list], columns=col_names)
            # check if the file exists
            file_exists = True if os.path.exists(out_file_path) else False
            if file_exists is False:
                with open(out_file_path, "w") as file:
                    data_row.to_csv(file, index=False, header=True)
            else:
                with open(out_file_path, "a") as file:
                    data_row.to_csv(file, index=False, header=False)
            # exit retry loop
            break

        # if a TooManyRequsts error is raised then the API rate limit has been exceeded.
        # Retry after sleeping. Sleep duration increases by a factor of 2 for 4 retries.
        except TooManyRequests as e:
            log_to_file(
                log_path, f'Error: {e} while fetching metadata for "{user_id}\n"'
            )
            print(f'Error: {e} while fetching metadata for "{user_id}"')
            sleep_time = 1 * (2**i)  # each retry waits for longer: 1s, 2s, 4s
            print(f"Making {i + 1}st retry after waiting {sleep_time}s")
            time.sleep(sleep_time)

        # catch all other possible exceptions and break retry loop
        except Exception as e:
            log_to_file(
                log_path,
                f'Unresolved Error: "{e}" while fetching "{user_id}"\'s metadata\n',
            )
            print(f'Error: "{e}" while fetching user {user_id}')
            break


"""Utilities for logging and data processing."""


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
