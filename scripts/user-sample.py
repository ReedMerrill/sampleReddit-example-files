"""An example script of how to perform snowball sampling from Reddit using the SSRA package.

Pre-requisites:
    1. A Reddit user account (the regular kind, for people to use the website)
    2. Reddit API credentials
        - follow this to set them up: https://github.com/reddit-archive/reddit/wiki/OAuth2-App-Types#script-app
        - after setting up your credentials, you will have a client_id, client_secret, and user_agent
    3. A version of Python 3 installed on your machine
    4. The sampleReddit python package is install
"""

import json
import time
import datetime
import sampleReddit as sr

# =============================================================================
# specify file paths for your project
# =============================================================================
# the path to your project directory
# PROJECT_PATH = "/path/to/your/project/directory/"
PROJECT_PATH = "/home/reed/Projects/learned-toxicity-reddit/reddit-api/"  # TESTING
# the path to the directory where you want your output files to be saved
# OUTPUT_PATH = f"{PROJECT_PATH}name of data folder/"  # TESTING
OUTPUT_PATH = f"{PROJECT_PATH}data/"
# where log files will be saved
LOG_FILE_PATH = f"{PROJECT_PATH}logs/"

# =============================================================================
# Set up authentication with the Reddit API
# =============================================================================

instance = sr.setup_access(
    client_id="your client id",
    client_secret="your client secret",
    password="your password",
    user_agent="your user agent",
    username="your Reddit username",
)

# =============================================================================
# set subreddit sample parameters
# =============================================================================
# the names of subreddits to sample
subreddits = ["politics", "Republican", "democrats"]
# How to filter posts within a subreddit. Can be "top", "new", or "hot".
filter = "top"
# How far back to go. Can be "all", "day", "hour", "month", "week", or "year" .
time_period = "year"
# the number of posts per subreddit to sample. Higher numbers significantly increase run time.
n_posts = 1

# =============================================================================
# Call the sampling function
# =============================================================================
# sample the subreddits
sampling_frame, users_df = sr.sample_reddit(
    api_instance=instance,
    seed_subreddits=subreddits,
    post_filter=filter,
    time_period=time_period,
    n_posts=n_posts,
    log_file_path=LOG_FILE_PATH,
)

# =============================================================================
# Save the data
# =============================================================================
# save the sampling frame
with open(f"{OUTPUT_PATH}sampling_frame.json", "w") as f:
    json.dump(sampling_frame, f, indent=2)
# save the users dataframe
users_df.to_csv(f"{OUTPUT_PATH}users.csv", index=False)
