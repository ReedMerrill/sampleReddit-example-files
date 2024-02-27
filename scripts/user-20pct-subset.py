"""Take a subset of the sample of users generated in snowball.py (and stored
in data/sample/user-sample.py).
"""

PROJECT_PATH = "/home/reed/Projects/learned-toxicity-reddit/reddit-api/"
INPUT_PATH = f"{PROJECT_PATH}data/sample/user-sample.csv"
OUTPUT_PATH = f"{PROJECT_PATH}data/sample/user-sample-subset-20pct.csv"

import pandas as pd
import random
import utils

users_raw = pd.read_csv(INPUT_PATH)
users = utils.process_user_ids(users_raw["users"])
users_subset = random.sample(users, k=round(len(users) * 0.2))

print(f"Original List Length: {len(users)}, Subset Length: {len(users_subset)}")

out = pd.DataFrame(users_subset, columns=["users"])

with open(OUTPUT_PATH, "w") as file:
    out.to_csv(file, index=False)
