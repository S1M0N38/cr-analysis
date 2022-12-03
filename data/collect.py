import argparse
import asyncio
import csv
import logging
import os
import pathlib
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
    "-o",
    "--output",
    action="store",
    type=pathlib.Path,
    default=here.parent / "db" / "test" / f'{now}.csv',
    help="Output path for .csv.",
)
parser.add_argument(
    "-f",
    "--force",
    action="store_true",
    help="Overwrite output file.",
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


args.output.parent.mkdir(parents=True, exist_ok=True)

csv_path: pathlib.Path = args.output
log_path: pathlib.Path = args.output.parent / "collect.log"

assert (
    not csv_path.exists() or args.force
), f"{csv_path} already exists. Use -f for overwrite it."
assert (
    csv_path.suffix == ".csv"
), "Output file will be a csv file. Use .csv as suffix for output."
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

    battlelogs.log.info("Start collecting ...")

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

    battlelogs.log.info("End collecting.")


asyncio.run(main())
