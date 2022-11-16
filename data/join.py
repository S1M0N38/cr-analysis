"""
THIS SCRIPT IS NO LONGER MANTAINED
I think it's better to develope small specific script like `join.sh` and `split.sh`.
They are faster and easier to mantain.
"""
import argparse
import csv
import gzip
import pathlib
import shutil
import sqlite3
import subprocess
from datetime import datetime

from tqdm import tqdm

here = pathlib.Path(__file__).parent
now = datetime.now().strftime("%Y%m%dT%H%M%S")

# ARGPARSE ----------------------------------------------------------------------------

parser = argparse.ArgumentParser(
    description=(
        "Join multiple .csv.gz into a sigle .csv removing duplicate rows. "
        "Eventually compress the result .csv into .csv.gz "
        "or convert into sqlite database."
    ),
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
    "-i",
    "--input-files",
    default="*.csv.gz",
    metavar="X",
    help="Pattern for input files.",
)
parser.add_argument(
    "-id",
    "--input-dir",
    action="store",
    type=pathlib.Path,
    required=True,
    help="Where to look for input files X.",
)
parser.add_argument(
    "-o",
    "--output-file",
    action="store",
    default=f"{now}.csv",
    metavar="Y",
    help="Name of the output file.",
)
parser.add_argument(
    "-od",
    "--output-dir",
    action="store",
    type=pathlib.Path,
    required=True,
    help="Where output file Y will be saved.",
)
parser.add_argument(
    "-r",
    "--remove",
    action="store_true",
    default=False,
    help="Remove input .gz.csv files after join.",
)
parser.add_argument(
    "-k",
    "--keep",
    action="store_true",
    default=False,
    help="Keep intermediate generated .csv",
)
parser.add_argument(
    "-f",
    "--force",
    action="store_true",
    default=False,
    help="Overwrite output file if exists.",
)
parser.add_argument(
    "--datetime-min",
    action="store",
    type=str,
    default="2000",
    help="Consider only battles play after datetime-min.",
)
parser.add_argument(
    "--datetime-max",
    action="store",
    type=str,
    default="3000",
    help="Consider only battles play before datetime-max.",
)
out_type = parser.add_mutually_exclusive_group()
out_type.add_argument(
    "-c",
    "--compress",
    action="store_true",
    default=False,
    help="Compress .csv.gz into a single .csv.gz",
)
out_type.add_argument(
    "-s",
    "--sqlite",
    action="store_true",
    default=False,
    help="Insert .csv.gz. files into sqlite database.",
)
args = parser.parse_args()


# MAIN --------------------------------------------------------------------------------

args.output_dir.mkdir(parents=True, exist_ok=True)

in_paths = list(args.input_dir.glob(args.input_files))
out_path = args.output_dir / args.output_file
battles_saved = set()

# TODO infer output based on out_path.suffix
if args.compress:
    assert "".join(out_path.suffixes) == ".csv.gz", "Output file must end with .csv.gz"
    csv_path = out_path.with_suffix("")
elif args.sqlite:
    assert out_path.suffix == ".sqlite", "Output file must end with .sqlite"
    csv_path = out_path.with_suffix(".csv")
else:
    assert out_path.suffix == ".csv", "Output file must end with .csv"
    csv_path = out_path

if not args.force:
    assert not out_path.exists(), "Output file already exists"
out_path.unlink(missing_ok=True)

with open(csv_path, "w", newline="") as csv_file:
    writer = csv.writer(csv_file)
    for gzip_path in tqdm(in_paths, position=1, ncols=0, disable=args.quiet):
        with gzip.open(gzip_path, "rt") as gzip_file:
            for battle in tqdm(
                csv.reader(gzip_file),
                desc=f"{gzip_path.name }",
                position=0,
                disable=args.quiet,
            ):
                if args.datetime_min < battle[0] < args.datetime_max:
                    if (hb := hash(tuple(battle))) in battles_saved:
                        continue
                    writer.writerow(battle)
                    battles_saved.add(hb)

if args.compress:

    # Compress csv to csv.gz
    with open(csv_path, "rb") as csv_file:
        with gzip.open(out_path, "wb") as gzip_file:
            shutil.copyfileobj(csv_file, gzip_file)

    # Check that compression was succefull
    with gzip.open(out_path, "rt") as gzip_file:
        assert sum(1 for _ in gzip_file) == len(battles_saved), "CompressionError"


elif args.sqlite:

    # Create battles table
    conn = sqlite3.connect(str(out_path))
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE battles (
        datetime TEXT, game_mode INT,
        tag1 TEXT, trophies1 INT, crowns1 INT,
        card11 INT, card12 INT, card13 INT, card14 INT,
        card15 INT, card16 INT, card17 INT, card18 INT,
        tag2 TEXT, trophies2 INT, crowns2 INT,
        card21 INT, card22 INT, card23 INT, card24 INT,
        card25 INT, card26 INT, card27 INT, card28 INT
        )"""
    )
    conn.close()

    subprocess.run(
        ["sqlite3", f"{out_path}", "-cmd", ".mode csv", f".import {csv_path} battles"],
    ).check_returncode()


# Remove intermediate .csv
if (args.compress or args.sqlite) and not args.keep:
    csv_path.unlink()


# Remove original .csv.gz files
if args.remove:
    for gzip_path in in_paths:
        gzip_path.unlink()
