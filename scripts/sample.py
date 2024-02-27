"""
A collection of wrappers for PRAW to aid in random sampling of Reddit data.
Includes utilities for sampling all major Reddit entities:
    - Subreddits
    - Posts
    - Comments
    - Users
"""

import time
import os
import praw
import pandas as pd
from prawcore.exceptions import TooManyRequests
import utils


# credentials
CLIENT_ID = "gB_we0e6sW9IK_Xjrw1szQ"
CLIENT_SECRET = "6rDRP5Hfu6SykadFTDZ8BMi4zSQNUg"
PASSWORD = "CmhtIf!rc-6ZmoJ"
USER_AGENT = "Reed's politics scraper v1.0.0 (u/bewchacca-lacca)"
USERNAME = "bewchacca-lacca"


def setup_access():
    """Create an instance for API access"""
    instance = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        password=PASSWORD,
        user_agent=USER_AGENT,
        username=USERNAME,
    )
    return instance


def get_top_posts(reddit, subreddit_name, time_period, n_submissions):
    """Takes the name of a subreddit, a time period, and the desired number of submissions
    and returns a list of the URLs of that subreddit's top posts.

    nb this is a separate function because doing so retains a list of the submissions, and parsing
    each level of the user extraction process into its own function avoids messy function outputs
    and is more efficient.
    Returns: list of top post URLs
    """
    # create the generator
    submission_generator = reddit.subreddit(subreddit_name).top(
        time_filter=time_period, limit=n_submissions
    )
    # return generator outputs as a list
    return [submission.id for submission in submission_generator]


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
            utils.log_to_file(
                log_path, f"Error: {e} while fetching one of {user_id}'s comments\n"
            )
            print(f"Error: {e} while fetching user {user_id}")
            sleep_time = 1 * (2**i)  # each retry waits for longer: 1s, 2s, 4s
            print(f"Making {i + 1}st retry after waiting {sleep_time}s")
            time.sleep(sleep_time)

        # catch all other possible exceptions and break retry loop
        except Exception as e:
            utils.log_to_file(
                log_path,
                f'Unresolved Error: "{e}" while fetching one of {user_id}\'s comments\n',
            )
            print(f'Error: "{e}" while fetching user {user_id}')
            break


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
            utils.log_to_file(
                log_path, f'Error: {e} while fetching metadata for "{user_id}\n"'
            )
            print(f'Error: {e} while fetching metadata for "{user_id}"')
            sleep_time = 1 * (2**i)  # each retry waits for longer: 1s, 2s, 4s
            print(f"Making {i + 1}st retry after waiting {sleep_time}s")
            time.sleep(sleep_time)

        # catch all other possible exceptions and break retry loop
        except Exception as e:
            utils.log_to_file(
                log_path,
                f'Unresolved Error: "{e}" while fetching "{user_id}"\'s metadata\n',
            )
            print(f'Error: "{e}" while fetching user {user_id}')
            break
