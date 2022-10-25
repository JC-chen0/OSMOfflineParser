import logging
import os
import traceback
from datetime import date, datetime
import yaml
import src.lines as lines
import src.rings as rings
import src.buildings as buildings
from argparse import ArgumentParser

from src.enum.hofn_type import HofnType
from src.enum.mcc import National
from src.enum.tag import Tag

VERSION = 2
DEBUG_VERSION = 1

if __name__ == "__main__":
    ###############################################
    # READ CONFIG
    config = dict()
    try:
        with open('config.yaml', 'r') as stream:
            config = yaml.safe_load(stream)
    except:
        pass
    rings_mode = config.get("mode").get("rings")
    lines_mode = config.get("mode").get("lines")
    DEBUGGING = config.get("debug")
    ALL_OFFLINE = config.get("all_offline")

    ###############################################
    # PROGRAM ARGUMENTS
    parser = ArgumentParser()
    # REQUIRED
    parser.add_argument("input", type=str, help="Input osm.pbf file path.", nargs="?")
    parser.add_argument("mcc", type=str, help="mcc", nargs="?")
    # parser.add_argument("nation", type=str, help="Nation name.")
    parser.add_argument("hofn_type", type=str, help="Process hofn type, Output file name", nargs="?")

    # OPTIONAL
    parser.add_argument("-v", "--version", help="Check current version", action="store_true")
    parser.add_argument("--limit_relation_id", type=str, help="If set, limit relation id will be changed from nation to id set.")
    parser.add_argument("--divide", type=str, help="format: id1, id2, id3 ..., if not set, all lines will not be divide.", nargs="+")
    parser.add_argument("--tags", type=str, help="format: tag_name1 search_value1 tag_name2 search_value2 ..., if not set, use default tags in config", nargs="+")
    args = parser.parse_args()

    if args.version:
        print(f"{VERSION}.{DEBUG_VERSION}")
        exit()
    #######################################
    # Loading program arguments
    input_path = args.input
    nation = National.get_country_by_mcc(args.mcc)
    limit_relation_id = args.limit_relation_id if args.limit_relation_id else National[nation].get_relation_id()
    divide = args.divide
    hofn_type = args.hofn_type
    mode = HofnType(hofn_type).name
    output_path = f"{config.get('path').get('output')}/{mode}/{nation}"
    if os.path.isdir(output_path) is not True:
        os.makedirs(output_path)

    # Grouping tags
    if args.tags:
        tags = {}
        tmp = 0
        while tmp < len(args.tags) - 1:
            tag = args.tags[tmp]
            value = args.tags[tmp + 1]
            tags[args.tags[tmp]] = args.tags[tmp + 1]
            tmp += 2
    else:
        tags = Tag[mode].value  # default value

    # road LEVEL_DICT
    highways_level = config.get("highways")
    ##########################################################################
    # DEBUG config
    try:
        with open('src/resource/logback.yaml', 'r') as stream:
            log_config = yaml.safe_load(stream)
            log_path = f"{config.get('path').get('log')}/{mode}"
            if not os.path.isdir(log_path):
                os.makedirs(log_path)
            log_config.get("handlers").get("info_file_handler")["filename"] = f"{log_path}/{limit_relation_id}-{date.today()}.info"
            log_config.get("handlers").get("debug_file_handler")["filename"] = f"{log_path}/{limit_relation_id}-{date.today()}.debug"
            logging.config.dictConfig(log_config)
    except Exception as e:
        logging.basicConfig(level=logging.DEBUG)
        traceback.print_exc()
        logging.debug("Error in Logging Configuration, Using default configs")

    ########################################################################
    logging.info("============================================")
    logging.info("Greetings fella, how's going?")
    logging.info(f"Start time: {datetime.now()}")
    logging.info("--------------------------------------------")
    logging.info(f"MODE: {mode}")
    logging.info(f"INPUT FILE PATH: {input_path}")
    logging.info(f"OUTPUT FILE PATH: {output_path}")
    logging.info(f"PROCESSING NATION: {nation}")
    logging.info(f"RELATION ID OF LIMIT AREA: {limit_relation_id}")
    logging.info(f"SEARCH TAG WITH VALUE: {tags}")
    logging.info(f"REMERGE AND DIVIDE: {True}") if divide else True
    logging.info(f"DEBUGGING: {DEBUGGING}") if DEBUGGING else True
    logging.info("--------------------------------------------")
    ##########################################################################
    if mode in rings_mode:
        rings.main(input_path=input_path, output_path=output_path, nation=nation, limit_relation_id=limit_relation_id, mode=mode, tags=tags, DEBUGGING=DEBUGGING, ALL_OFFLINE=ALL_OFFLINE)
    elif mode in lines_mode:
        if mode == "highway":
            lines.main(input_path=input_path, output_path=output_path, nation=nation, limit_relation_id=limit_relation_id, mode=mode, tags=tags, DIVIDE=divide, LEVEL_DICT=highways_level, DEBUGGING=DEBUGGING, ALL_OFFLINE=ALL_OFFLINE)
        else:
            lines.main(input_path=input_path, output_path=output_path, nation=nation, limit_relation_id=limit_relation_id, mode=mode, tags=tags, DIVIDE=divide, DEBUGGING=DEBUGGING, ALL_OFFLINE=ALL_OFFLINE)
    elif mode == "building":
        buildings.main(input_path, output_path, nation, limit_relation_id, DEBUGGING, ALL_OFFLINE)
