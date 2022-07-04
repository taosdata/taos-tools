/*
* Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
*
* This program is free software: you can use, redistribute, and/or modify
* it under the terms of the MIT license as published by the Free Software
* Foundation.
*
* This program is distributed in the hope that it will be useful, but WITHOUT
* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
* FITNESS FOR A PARTICULAR PURPOSE.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#include "bench.h"
const char *              argp_program_version;
const char *              argp_program_bug_address = "<support@taosdata.com>";
static char               doc[] = "";
static char               args_doc[] = "";
static struct argp_option options[] = {
    {"file", 'f', "FILE", 0,
     "(**IMPORTANT**) Set JSON configuration file(all options are going to "
     "read from this JSON file), which is mutually exclusive with other "
     "commandline options, examples are under /usr/local/taos/examples",
     0},
    {"config-dir", 'c', "CONFIG_DIR", 0, "Configuration directory.", 1},
    {"host", 'h', "HOST", 0,
     "TDengine server FQDN to connect, default is localhost."},
    {"port", 'P', "PORT", 0,
     "The TCP/IP port number to use for the connection, default is 6030."},
    {"interface", 'I', "IFACE", 0,
     "insert mode, default is taosc, options: taosc|rest|stmt|sml"},
    {"user", 'u', "USER", 0,
     "The user name to use when connecting to the server, default is root."},
    {"password", 'p', "PASSWORD", 0,
     "The password to use when connecting to the server, default is taosdata."},
    {"output", 'o', "FILE", 0,
     "The path of result output file, default is ./output.txt."},
    {"threads", 'T', "NUMBER", 0,
     "The number of thread when insert data, default is 8."},
    {"insert-interval", 'i', "NUMBER", 0,
     "Insert interval for interlace mode in milliseconds, default is 0."},
    {"time-step", 'S', "NUMBER", 0,
     "Timestamp step in milliseconds, default is 1."},
    {"interlace-rows", 'B', "NUMBER", 0,
     "The number of interlace rows insert into tables, default is 0"},
    {"rec-per-req", 'r', "NUMBER", 0,
     "Number of records in each insert request, default is 30000."},
    {"tables", 't', "NUMBER", 0, "Number of child tables, default is 10000."},
    {"records", 'n', "NUMBER", 0,
     "Number of records for each table, default is 10000."},
    {"database", 'd', "DATABASE", 0, "Name of database, default is test."},
    {"columns", 'l', "NUMBER", 0,
     "Number of INT data type columns in table, default is 0. "},
    {"tag-type", 'A', "TAG_TYPE", 0,
     "Data type of tables' tags, default is INT,BINARY(16)."},
    {"data-type", 'b', "COL_TYPE", 0,
     "Data type of tables' cols, default is FLOAT,INT,FLOAT."},
    {"binwidth", 'w', "NUMBER", 0,
     "The default length of nchar and binary if not specified, default is "
     "64."},
    {"table-prefix", 'm', "TABLE_PREFIX", 0,
     "Prefix of child table name, default is d."},
    {"escape-character", 'E', 0, 0,
     "Use escape character in stable and child table name, optional."},
    {"chinese", 'C', 0, 0,
     "Nchar and binary are basic unicode chinese characters, optional."},
    {"normal-table", 'N', 0, 0,
     "Only create normal table without super table, optional."},
    {"random", 'M', 0, 0, "Data source is randomly generated, optional."},
    {"aggr-func", 'x', 0, 0,
     "Query aggregation function after insertion, optional."},
    {"answer-yes", 'y', 0, 0,
     "Pass confirmation prompt to continue, optional."},
    {"disorder-range", 'R', "NUMBER", 0,
     "Range of disordered timestamp, default is 1000."},
    {"disorder", 'O', "NUMBER", 0,
     "Ratio of inserting data with disorder timestamp, default is 0."},
    {"replia", 'a', "NUMBER", 0,
     "The number of replica when create database, default is 1."},
    {"debug", 'g', 0, 0, "Debug mode, optional."},
    {"performance", 'G', 0, 0, "Performance mode, optional."},
    {"prepared_rand", 'F', "NUMBER", 0,
     "Random data source size, default is 10000."},
    {"connection_pool", 'H', "NUMBER", 0,
     "size of the pre-connected client in connection pool, default is 8"},
    {0}};

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  SArguments *arguments = state->input;
  switch (key) {
    case 'F':
      arguments->prepared_rand = atol(arg);
      if (arguments->prepared_rand <= 0) {
        errorPrint(stderr,
                   "Invalid -F: %s, will auto set to default(10000)\n",
                   arg);
        arguments->prepared_rand = DEFAULT_PREPARED_RAND;
      }
      break;
    case 'f':
      arguments->demo_mode = false;
      arguments->metaFile = arg;
      break;
    case 'h':
      arguments->host = arg;
      break;
    case 'P':
      arguments->port = atoi(arg);
      if (arguments->port <= 0) {
        errorPrint(stderr,
                   "Invalid -P: %s, will auto set to default(6030)\n",
                   arg);
        arguments->port = DEFAULT_PORT;
      }
      break;
    case 'I':
      if (0 == strcasecmp(arg, "taosc")) {
        arguments->db->superTbls->iface = TAOSC_IFACE;
      } else if (0 == strcasecmp(arg, "stmt")) {
        arguments->db->superTbls->iface = STMT_IFACE;
      } else if (0 == strcasecmp(arg, "rest")) {
        arguments->db->superTbls->iface = REST_IFACE;
      } else if (0 == strcasecmp(arg, "sml")) {
        arguments->db->superTbls->iface = SML_IFACE;
      } else {
        errorPrint(stderr,
                   "Invalid -I: %s, will auto set to default (taosc)\n",
                   arg);
        arguments->db->superTbls->iface = TAOSC_IFACE;
      }
      break;
    case 'p':
      arguments->password = arg;
      break;
    case 'u':
      arguments->user = arg;
      break;
    case 'c':
      tstrncpy(configDir, arg, TSDB_FILENAME_LEN);
      break;
    case 'o':
      arguments->output_file = arg;
      break;
    case 'T':
      arguments->nthreads = atoi(arg);
      if (arguments->nthreads <= 0) {
        errorPrint(stderr,
                   "Invalid -T: %s, will auto set to default(8)\n",
                   arg);
        arguments->nthreads = DEFAULT_NTHREADS;
      }
      break;
    case 'H':
      arguments->connection_pool = atoi(arg);
      if (arguments->connection_pool <= 0) {
        errorPrint(stderr,
                   "Invalid -H: %s, will auto set to default(8)\n",
                   arg);
        arguments->connection_pool = DEFAULT_NTHREADS;
      }
      break;
    case 'i':
      arguments->db->superTbls->insert_interval = atoi(arg);
      if (arguments->db->superTbls->insert_interval <= 0) {
        errorPrint(stderr,
                   "Invalid -i: %s, will auto set to default(0)\n",
                   arg);
        arguments->db->superTbls->insert_interval = 0;
      }
      break;
    case 'S':
      arguments->db->superTbls->timestamp_step = atol(arg);
      if (arguments->db->superTbls->timestamp_step <= 0) {
        errorPrint(stderr,
                   "Invalid -S: %s, will auto set to default(1)\n",
                   arg);
        arguments->db->superTbls->timestamp_step = 1;
      }
      break;
    case 'B':
      arguments->db->superTbls->interlaceRows = atoi(arg);
      if (arguments->db->superTbls->interlaceRows <= 0) {
        errorPrint(stderr,
                   "Invalid -B: %s, will auto set to default(0)\n",
                   arg);
        arguments->db->superTbls->interlaceRows = 0;
      }
      break;
    case 'r':
      arguments->reqPerReq = atoi(arg);
      if (arguments->reqPerReq <= 0 ||
          arguments->reqPerReq > MAX_RECORDS_PER_REQ) {
        errorPrint(stderr,
                   "Invalid -r: %s, will auto set to default(30000)\n",
                   arg);
        arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
      }
      break;
    case 't':
      arguments->db->superTbls->childTblCount = atoi(arg);
      if (arguments->db->superTbls->childTblCount <= 0) {
        errorPrint(stderr,
                   "Invalid -t: %s, will auto set to default(10000)\n",
                   arg);
        arguments->db->superTbls->childTblCount = DEFAULT_CHILDTABLES;
      }
      g_arguments->g_totalChildTables =
          arguments->db->superTbls->childTblCount;
      break;
    case 'n':
      arguments->db->superTbls->insertRows = atol(arg);
      if (arguments->db->superTbls->insertRows <= 0) {
        errorPrint(stderr,
                   "Invalid -n: %s, will auto set to default(10000)\n",
                   arg);
        arguments->db->superTbls->insertRows = DEFAULT_INSERT_ROWS;
      }
      break;
    case 'd':
      arguments->db->dbName = arg;
      break;
    case 'l':
      arguments->demo_mode = false;
      arguments->intColumnCount = atoi(arg);
      if (arguments->intColumnCount <= 0) {
        errorPrint(stderr,
                   "Invalid -l: %s, will auto set to default(0)\n",
                   arg);
        arguments->intColumnCount = 0;
      }
      break;
    case 'A':
      arguments->demo_mode = false;
      count_datatype(arg, &(arguments->db->superTbls->tagCount));
      tmfree(arguments->db->superTbls->tags);
      arguments->db->superTbls->tags =
              benchCalloc(arguments->db->superTbls->tagCount, sizeof(Column), true);
      if (parse_tag_datatype(arg, arguments->db->superTbls->tags)) {
        tmfree(arguments->db->superTbls->tags);
        exit(EXIT_FAILURE);
      }
      break;
    case 'b':
      arguments->demo_mode = false;
      tmfree(arguments->db->superTbls->columns);
      count_datatype(arg, &(arguments->db->superTbls->columnCount));
      arguments->db->superTbls->columns =
              benchCalloc(arguments->db->superTbls->columnCount, sizeof(Column), true);
      if (parse_col_datatype(arg, arguments->db->superTbls->columns)) {
        tmfree(arguments->db->superTbls->columns);
        exit(EXIT_FAILURE);
      }
      break;
    case 'w':
      arguments->binwidth = atoi(arg);
      if (arguments->binwidth <= 0) {
        errorPrint(
            stderr,
            "Invalid value for w: %s, will auto set to default(64)\n",
            arg);
        arguments->binwidth = DEFAULT_BINWIDTH;
      } else if (arguments->binwidth > TSDB_MAX_BINARY_LEN) {
        errorPrint(stderr,
                   "-w(%d) > TSDB_MAX_BINARY_LEN(%" PRIu64
                   "), will auto set to default(64)\n",
                   arguments->binwidth, (uint64_t)TSDB_MAX_BINARY_LEN);
        arguments->binwidth = DEFAULT_BINWIDTH;
      }
      break;
    case 'm':
      arguments->db->superTbls->childTblPrefix = arg;
      break;
    case 'E':
      arguments->db->superTbls->escape_character = true;
      break;
    case 'C':
      arguments->chinese = true;
      break;
    case 'N':
      arguments->demo_mode = false;
      arguments->db->superTbls->use_metric = false;
      arguments->db->superTbls->tagCount = 0;
      break;
    case 'M':
      arguments->demo_mode = false;
      break;
    case 'x':
      arguments->aggr_func = true;
      break;
    case 'y':
      arguments->answer_yes = true;
      break;
    case 'R':
      arguments->db->superTbls->disorderRange = atoi(arg);
      if (arguments->db->superTbls->disorderRange <= 0) {
        errorPrint(stderr,
                   "Invalid value for -R: %s, will auto set to "
                   "default(1000)\n",
                   arg);
        arguments->db->superTbls->disorderRange =
            DEFAULT_DISORDER_RANGE;
      }
      break;
    case 'O':
      arguments->db->superTbls->disorderRatio = atoi(arg);
      if (arguments->db->superTbls->disorderRatio <= 0) {
        errorPrint(
            stderr,
            "Invalid value for -O: %s, will auto set to default(0)\n",
            arg);
        arguments->db->superTbls->disorderRatio = 0;
      }
      break;
    case 'a':
      arguments->db->dbCfg.replica = atoi(arg);
      if (arguments->db->dbCfg.replica <= 0) {
        errorPrint(
            stderr,
            "Invalid value for -a: %s, will auto set to default(1)\n",
            arg);
        arguments->db->dbCfg.replica = 1;
      }
      break;
    case 'g':
      arguments->debug_print = true;
      break;
    case 'G':
      arguments->performance_print = true;
      break;
    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

static struct argp argp = {options, parse_opt, args_doc, doc};

void commandLineParseArgument(int argc, char *argv[]) {
  argp_program_version = taos_get_client_info();
  argp_parse(&argp, argc, argv, 0, 0, g_arguments);
}