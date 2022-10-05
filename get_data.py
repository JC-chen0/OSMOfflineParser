import os
import src.lines as lines
import src.rings as rings
from argparse import ArgumentParser
from src.enum.mcc import National

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("input", type=str, help="Input osm.pbf file path.")
    parser.add_argument("nation", type=str, help="Nation name.")
    parser.add_argument("--limit_relation_id", type=str, help="If set, limit relation id will be changed from nation to id set.")
    parser.add_argument("--divide", const=True, default=False, nargs="?")  # Set as a flag
    parser.add_argument("--mode", type=str, help="Process mode, Output file name")
    parser.add_argument("--tags", type=str, help="format: tag_name1 search_value1 tag_name2 search_value2 ...", nargs="+")

    args = parser.parse_args()
    input_path = args.input
    nation = args.nation
    limit_relation_id = args.limit_relation_id if args.limit_relation_id else National[nation].get_relation_id()
    divide = args.divide
    mode = args.mode
    output_path = f"data/output/{mode}/{nation}"
    if os.path.isdir(output_path) is not True:
        os.makedirs(output_path)

    # Grouping tags
    tags = {}
    tmp = 0
    while tmp < len(args.tags) - 1:
        tag = args.tags[tmp]
        value = args.tags[tmp + 1]
        tags[args.tags[tmp]] = args.tags[tmp + 1]
        tmp += 2

    # config
    DEBUGGING = False
    # mode
    rings_mode = ["water", "village"]
    lines_mode = ["coastline", "highway"]

    # road level
    highways_type = ["motorway", "trunk", "primary", "secondary", "tertiary"]
    highways_level = dict(zip(highways_type, [1, 2, 3, 4, 5]))

    if mode in rings_mode:
        rings.main(input_path, output_path, nation, limit_relation_id, mode, tags)
    elif mode in lines_mode:
        lines.main(input_path, output_path, nation, limit_relation_id, divide, mode, tags)

