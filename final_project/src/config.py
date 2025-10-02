from dotenv import load_dotenv
import os
load_dotenv()

PRODUCTHUNT_TOKEN = os.getenv("PRODUCTHUNT_TOKEN")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

DATA_DIR = "data"
FIG_DIR = "figs"
