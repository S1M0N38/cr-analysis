import argparse
import asyncio
import csv
import gzip
import logging
import pathlib
import shutil
from datetime import datetime

import aiohttp
import tqdm
from crawler import Crawler
from tokens import TOKENS

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


async def get_token():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://wtfismyip.com/text") as resp:
            ip = await resp.text()
    api_token = TOKENS.get(ip.strip())
    assert api_token, f"{ip} does not have token"
    return api_token


battlelogs = Crawler(
    api_token=asyncio.run(get_token()),
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
