import logging
import os
import traceback
from datetime import date

import yaml

import src.lines as lines
import src.rings as rings
from argparse import ArgumentParser
from src.enum.mcc import National
from src.enum.tag import Tag

if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("input", type=str, help="Input osm.pbf file path.")
    parser.add_argument("nation", type=str, help="Nation name.")
    parser.add_argument("--limit_relation_id", type=str, help="If set, limit relation id will be changed from nation to id set.")
    parser.add_argument("--divide", const=True, default=False, nargs="?")  # Set as a flag
    parser.add_argument("--mode", type=str, help="Process mode, Output file name")

    args = parser.parse_args()
    input_path = args.input
    nation = args.nation
    limit_relation_id = args.limit_relation_id if args.limit_relation_id else National[nation].get_relation_id()
    divide = args.divide
    mode = args.mode
    output_path = f"data/output/{mode}/{nation}"
    if os.path.isdir(output_path) is not True:
        os.makedirs(output_path)
    tags = Tag[mode].value
    # Grouping tags
    # tags = {}
    # tmp = 0
    # while tmp < len(args.tags) - 1:
    #     tag = args.tags[tmp]
    #     value = args.tags[tmp + 1]
    #     tags[args.tags[tmp]] = args.tags[tmp + 1]
    #     tmp += 2

    # config
    DEBUGGING = False
    # mode
    rings_mode = ["water", "village"]
    lines_mode = ["coastline", "highway"]

    # road level
    highways_type = ["motorway", "trunk", "primary", "secondary", "tertiary"]
    highways_level = dict(zip(highways_type, [1, 2, 3, 4, 5]))

    try:
        with open('src/resource/logback.yaml', 'r') as stream:
            config = yaml.safe_load(stream)
            log_path = f"logs/{mode}"
            if not os.path.isdir(log_path):
                os.makedirs(log_path)
            config.get("handlers").get("info_file_handler")["filename"] = f"{log_path}/{limit_relation_id}-{date.today()}.info"
            config.get("handlers").get("debug_file_handler")["filename"] = f"{log_path}/{limit_relation_id}-{date.today()}.debug"
            logging.config.dictConfig(config)
    except Exception as e:
        logging.basicConfig(level=logging.DEBUG)
        traceback.print_exc()
        logging.debug("Error in Logging Configuration, Using default configs")

    logging.info("============================================")
    logging.info(f"MODE: {mode}")
    logging.info(f"INPUT FILE PATH: {input_path}")
    logging.info(f"OUTPUT FILE PATH: {output_path}")
    logging.info(f"PROCESSING NATION: {nation}")
    logging.info(f"RELATION ID OF LIMIT AREA: {limit_relation_id}")
    logging.info(f"SEARCH TAG WITH VALUE: {tags}")
    logging.info(f"REMERGE AND DIVIDE: {divide}") if divide else True
    logging.info("============================================")

    if mode in rings_mode:
        rings.main(input_path, output_path, nation, limit_relation_id, mode, tags)
    elif mode in lines_mode:
        lines.main(input_path, output_path, nation, limit_relation_id, divide, mode, tags)
