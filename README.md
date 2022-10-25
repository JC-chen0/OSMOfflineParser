# OSM offline module

## PURPOSE:

Read offline .osm.pbf file to generate NT2_GEO_POLYGON.tsv

## DATA SOURCE:
https://download.geofabrik.de/

## USER GUIDE:

1. Get_data can get geo data with specific hofn type.
2. Gen geo polygon can get NT2_GEO_POLYGON with specific hofn types.

Arguments
### get_data

```shell=
usage: get_data.py [-h] [--limit_relation_id LIMIT_RELATION_ID]
                   [--divide DIVIDE [DIVIDE ...]] [--tags TAGS [TAGS ...]]
                   [--debug [DEBUG]] [--all_offline [ALL_OFFLINE]]
                   input mcc hofn_type

positional arguments:
  input                 Input osm.pbf file path.
  mcc                   mcc
  hofn_type             Process hofn type, Output file name

optional arguments:
  -h, --help            show this help message and exit
  --limit_relation_id LIMIT_RELATION_ID
                        If set, limit relation id will be changed from nation
                        to id set.
  --divide DIVIDE [DIVIDE ...]
                        format: id1, id2, id3 ...
  --tags TAGS [TAGS ...]
                        format: tag_name1 search_value1 tag_name2
                        search_value2 ...
  --debug [DEBUG]       ONLY generate geojson file
  --all_offline [ALL_OFFLINE]
                        DEFAULT true, but cost more time, False will using
                        overpy to get data```

### gen_geo_polygon

```shell=
usage: gen_geo_polygon.py [-h] [--get_data [GET_DATA]] mcc hofn_types

positional arguments:
  mcc                   mcc
  hofn_types            format: 'HofnType1 HofnType2' ...

optional arguments:
  -h, --help            show this help message and exit
  --get_data [GET_DATA]
```

## Refer:

[Landusage](http://redmine.ghtinc.com/projects/chtcovms/wiki/Landusage)