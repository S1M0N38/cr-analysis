import argparse
import asyncio
import csv
import gzip
import logging
import os
import pathlib
import shutil
from datetime import datetime

import httpx
import tqdm
from crawler import Crawler

# PATHS -------------------------------------------------------------------------------

here = pathlib.Path(__file__).parent
now = datetime.now().strftime("%Y%m%dT%H%M%S")

# ARGPARSE ----------------------------------------------------------------------------

parser = argparse.ArgumentParser(
    description="Collect Clash Royale battles from top ladder.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-q",
    "--quiet",
    action="store_true",
    default=False,
    help="Disable progress bar.",
)
parser.add_argument(
    "-p",
    "--players",
    action="store",
    metavar="X",
    type=int,
    default=float("inf"),
    help="Stop crawler after X players.",
)
parser.add_argument(
    "-rp",
    "--root-players",
    type=str,
    default=["G9YV9GR8R", "Y9R22RQ2", "R90PRV0PY", "RVCQ2CQGJ"],
    nargs="+",
    help="List of root players",
)
parser.add_argument(
    "-r",
    "--requests",
    action="store",
    metavar="Y",
    type=int,
    default=13,
    help="Perform up to Y http requests concurently.",
)
parser.add_argument(
    "-d",
    "--database-dir",
    action="store",
    type=pathlib.Path,
    default=here / "db-hour",
    help="Where .csv/.csv.gz will be stored.",
)
parser.add_argument(
    "-c",
    "--compress",
    action="store_true",
    default=False,
    help="Compress .csv into .csv.gz.",
)
parser.add_argument(
    "-k",
    "--keep-original",
    action="store_true",
    default=False,
    help="Keep the original .csv after compression.",
)
parser.add_argument(
    "-v",
    "--verbose",
    action="count",
    default=1,
    help="Increase console log level.",
)

args = parser.parse_args()

# convert v counts into logging level https://gist.github.com/ms5/9f6df9c42a5f5435be0e
args.verbose = 40 - (10 * args.verbose) if args.verbose > 0 else 0

# CONFIGS -----------------------------------------------------------------------------

args.database_dir.mkdir(parents=True, exist_ok=True)
csv_path = args.database_dir / f"{now}.csv"
gzip_path = args.database_dir / f"{now}.csv.gz"
log_path = args.database_dir / "collect.log"

assert (
    email := os.getenv("API_CLASH_ROYALE_EMAIL")
), "API_CLASH_ROYALE_EMAIL env variable is not define"
assert (
    password := os.getenv("API_CLASH_ROYALE_PASSWORD")
), "API_CLASH_ROYALE_PASSWORD env variable is not define"

ip = httpx.get("https://wtfismyip.com/text").text.strip()

credentials = {"email": email, "password": password}
api_key = {
    "name": "cr-analysis",
    "description": f"API key automatically generated at {now}",
    "cidrRanges": [ip],
    "scope": None,
}

with httpx.Client(base_url="https://developer.clashroyale.com") as client:
    client.post("/api/login", json=credentials)
    keys = client.post("/api/apikey/list", json={}).json().get("keys", [])
    if len(keys) == 10:
        client.post("/api/apikey/revoke", json={"id": keys[-1]["id"]})
    api_token = client.post("/api/apikey/create", json=api_key).json()["key"]["key"]

battlelogs = Crawler(
    api_token=api_token,
    trophies_ranked_target=10_000,
    trophies_ladder_target=10_000,
    root_players=args.root_players,
    battlelogs_limit=args.players,
    concurrent_requests=args.requests,
    log_level_console=args.verbose,
    log_level_file=logging.INFO,
    log_file_path=log_path,
)


# MAIN --------------------------------------------------------------------------------


async def main():

    battles_saved = set()

    progress_bar = tqdm.tqdm(
        total=args.players,
        disable=args.quiet,
        ncols=0,
        unit=" players",
        smoothing=0.05,
    )

    with open(csv_path, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        async for battles in battlelogs:
            for b in battles:
                if b.player1.tag > b.player2.tag:
                    battle = (b.battle_time, b.game_mode, *b.player1, *b.player2)
                else:
                    battle = (b.battle_time, b.game_mode, *b.player2, *b.player1)
                if (hb := hash(battle)) not in battles_saved:
                    writer.writerow(battle)
                    battles_saved.add(hb)
            progress_bar.update()
    progress_bar.close()

    if args.compress:
        with open(csv_path, "rb") as csv_file:
            with gzip.open(gzip_path, "wb") as gzip_file:
                shutil.copyfileobj(csv_file, gzip_file)

        # Check that compression was succefull
        with gzip.open(gzip_path, "rt") as gzip_file:
            assert sum(1 for _ in gzip_file) == len(battles_saved), "CompressionError"

        if not args.keep_original:
            csv_path.unlink()


asyncio.run(main())
