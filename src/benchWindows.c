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

inline void exit_required(char* name) {
    errorPrint(stderr, "option %s requires an arguments\n", name);
    exit(EXIT_FAILURE);
}

void printUsage() {
    fprintf(stdout, "Usage: taosdemo [-CEgGMNxy?V] [-f FILE] [-a NUMBER] [-A TAG_TYPE]\n"
                    "            [-b COL_TYPE] [-B NUMBER] [-c CONFIG_DIR] [-d DATABASE]\n"
                    "            [-F NUMBER] [-h HOST] [-H NUMBER] [-i NUMBER] [-I IFACE]\n"
                    "            [-l NUMBER] [-m TABLE_PREFIX] [-n NUMBER] [-o FILE] [-O NUMBER]\n"
                    "            [-p PASSWORD] [-P PORT] [-r NUMBER] [-R NUMBER] [-S NUMBER]\n"
                    "            [-t NUMBER] [-T NUMBER] [-u USER] [-w NUMBER] [--file=FILE]\n"
                    "            [--replia=NUMBER] [--tag-type=TAG_TYPE] [--data-type=COL_TYPE]\n"
                    "            [--interlace-rows=NUMBER] [--config-dir=CONFIG_DIR] [--chinese]\n"
                    "            [--database=DATABASE] [--escape-character] [--prepared_rand=NUMBER]\n"
                    "            [--debug] [--performance] [--host=HOST] [--connection_pool=NUMBER]\n"
                    "            [--insert-interval=NUMBER] [--interface=IFACE] [--columns=NUMBER]\n"
                    "            [--table-prefix=TABLE_PREFIX] [--random] [--records=NUMBER]\n"
                    "            [--normal-table] [--output=FILE] [--disorder=NUMBER]\n"
                    "            [--password=PASSWORD] [--port=PORT] [--rec-per-req=NUMBER]\n"
                    "            [--disorder-range=NUMBER] [--time-step=NUMBER] [--tables=NUMBER]\n"
                    "            [--threads=NUMBER] [--user=USER] [--binwidth=NUMBER] [--aggr-func]\n"
                    "            [--answer-yes] [--help] [--usage] [--version]");
}

void printHelp() {
  fprintf(stdout, "Usage: taosdemo [OPTION...] \n"
      "\n"
      "  -f, --file=FILE            (**IMPORTANT**) Set JSON configuration file(all\n"
      "                             options are going to read from this JSON file),\n"
      "                             which is mutually exclusive with other commandline\n"
      "                             options, examples are under\n"
      "                             /usr/local/taos/examples\n"
      "  -a, --replia=NUMBER        The number of replica when create database,\n"
      "                             default is 1.\n"
      "  -A, --tag-type=TAG_TYPE    Data type of tables' tags, default is\n"
      "                             INT,BINARY(16).\n"
      "  -b, --data-type=COL_TYPE   Data type of tables' cols, default is\n"
      "                             FLOAT,INT,FLOAT.\n"
      "  -B, --interlace-rows=NUMBER   The number of interlace rows insert into\n"
      "                             tables, default is 0\n"
      "  -c, --config-dir=CONFIG_DIR   Configuration directory.\n"
      "  -C, --chinese              Nchar and binary are basic unicode chinese\n"
      "                             characters, optional.\n"
      "  -d, --database=DATABASE    Name of database, default is test.\n"
      "  -E, --escape-character     Use escape character in stable and child table\n"
      "                             name, optional.\n"
      "  -F, --prepared_rand=NUMBER Random data source size, default is 10000.\n"
      "  -g, --debug                Debug mode, optional.\n"
      "  -G, --performance          Performance mode, optional.\n"
      "  -h, --host=HOST            TDengine server FQDN to connect, default is\n"
      "                             localhost.\n"
      "  -H, --connection_pool=NUMBER   size of the pre-connected client in connection\n"
      "                             pool, default is 8\n"
      "  -i, --insert-interval=NUMBER   Insert interval for interlace mode in\n"
      "                             milliseconds, default is 0.\n"
      "  -I, --interface=IFACE      insert mode, default is taosc, options:\n"
      "                             taosc|rest|stmt|sml\n"
      "  -l, --columns=NUMBER       Number of INT data type columns in table, default\n"
      "                             is 0. \n"
      "  -m, --table-prefix=TABLE_PREFIX\n"
      "                             Prefix of child table name, default is d.\n"
      "  -M, --random               Data source is randomly generated, optional.\n"
      "  -n, --records=NUMBER       Number of records for each table, default is\n"
      "                             10000.\n"
      "  -N, --normal-table         Only create normal table without super table,\n"
      "                             optional.\n"
      "  -o, --output=FILE          The path of result output file, default is\n"
      "                             ./output.txt.\n"
      "  -O, --disorder=NUMBER      Ratio of inserting data with disorder timestamp,\n"
      "                             default is 0.\n"
      "  -p, --password=PASSWORD    The password to use when connecting to the server,\n"
      "                             default is taosdata.\n"
      "  -P, --port=PORT            The TCP/IP port number to use for the connection,\n"
      "                             default is 6030.\n"
      "  -r, --rec-per-req=NUMBER   Number of records in each insert request, default\n"
      "                             is 30000.\n"
      "  -R, --disorder-range=NUMBER   Range of disordered timestamp, default is 1000.\n"
      "                            \n"
      "  -S, --time-step=NUMBER     Timestamp step in milliseconds, default is 1.\n"
      "  -t, --tables=NUMBER        Number of child tables, default is 10000.\n"
      "  -T, --threads=NUMBER       The number of thread when insert data, default is\n"
      "                             8.\n"
      "  -u, --user=USER            The user name to use when connecting to the\n"
      "                             server, default is root.\n"
      "  -w, --binwidth=NUMBER      The default length of nchar and binary if not\n"
      "                             specified, default is 64.\n"
      "  -x, --aggr-func            Query aggregation function after insertion,\n"
      "                             optional.\n"
      "  -y, --answer-yes           Pass confirmation prompt to continue, optional.\n"
      "  -?, --help                 Give this help list\n"
      "      --usage                Give a short usage message\n"
      "  -V, --version              Print program version\n"
      "\n"
      "Mandatory or optional arguments to long options are also mandatory or optional\n"
      "for any corresponding short options.\n"
      "\n"
      "Report bugs to <support@taosdata.com>.");
}

void commandLineParseArgument(int argc, char *argv[]) {
    SDataBase * database = benchArrayGet(g_arguments->databases, 0);
    SSuperTable * stbInfo = benchArrayGet(database->superTbls, 0);
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-F") == 0) {
            if (i < argc - 1) {
                g_arguments->prepared_rand = atol(argv[++i]);
            } else {
                exit_required("-F");
            }
        } else if (strcmp(argv[i], "-f") == 0) {
            if (i < argc - 1) {
                g_arguments->demo_mode = false;
                g_arguments->metaFile = argv[++i];
            } else {
                exit_required("-f");
            }
        } else if (strcmp(argv[i], "-h") == 0) {
            if (i < argc - 1) {
                g_arguments->host = argv[++i];
            } else {
                exit_required("-h");
            }
        } else if (strcmp(argv[i], "-P") == 0) {
            if (i < argc -1) {
                g_arguments->port = atoi(argv[++i]);
            } else {
                exit_required("-P");
            }
        } else if (strcmp(argv[i], "-I") == 0) {
            if (i < argc -1) {
                char* mode = argv[++i];
                if (0 == strcasecmp(mode, "taosc")) {
                    stbInfo->iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(mode, "stmt")) {
                    stbInfo->iface = STMT_IFACE;
                } else if (0 == strcasecmp(mode, "rest")) {
                    stbInfo->iface = REST_IFACE;
                } else if (0 == strcasecmp(mode, "sml")) {
                    stbInfo->iface = SML_IFACE;
                } else {
                    errorPrint(stderr,
                               "Invalid -I: %s, will auto set to default (taosc)\n",
                               mode);
                    stbInfo->iface = TAOSC_IFACE;
                }
            } else {
                exit_required("-I");
            }
        } else if (strcmp(argv[i], "-p") == 0) {
            if (i < argc - 1) {
                g_arguments->password = argv[++i];
            } else {
                exit_required("-p");
            }
        } else if (strcmp(argv[i], "-u") == 0) {
            if (i < argc - 1) {
                g_arguments->user = argv[++i];
            } else {
                exit_required("-u");
            }
        } else if (strcmp(argv[i], "-c") == 0) {
            if (i < argc - 1) {
                tstrncpy(configDir, argv[++i], TSDB_FILENAME_LEN);
            } else {
                exit_required("-c");
            }
        } else if (strcmp(argv[i], "-o") == 0) {
            if (i < argc - 1) {
                g_arguments->output_file = argv[++i];
            } else {
                exit_required("-o");
            }
        } else if (strcmp(argv[i], "-T") == 0) {
            if (i < argc - 1) {
                g_arguments->nthreads = atoi(argv[++i]);
            } else {
                exit_required("-T");
            }
        } else if (strcmp(argv[i], "-H") == 0) {
            if (i < argc - 1) {
                g_arguments->connection_pool = atoi(argv[++i]);
            } else {
                exit_required("-H");
            }
        } else if (strcmp(argv[i], "-i") == 0) {
            if (i < argc - 1) {
                stbInfo->insert_interval = atoi(argv[++i]);
            } else {
                exit_required("-i");
            }
        } else if (strcmp(argv[i], "-S") == 0) {
            if (i < argc - 1) {
                stbInfo->timestamp_step = atol(argv[++i]);
            } else {
                exit_required("-S");
            }
        } else if (strcmp(argv[i], "-B") == 0) {
            if (i < argc - 1) {
                stbInfo->interlaceRows = atoi(argv[++i]);
            } else {
                exit_required("-b");
            }
        } else if (strcmp(argv[i], "-r") == 0) {
            if (i < argc - 1) {
                g_arguments->reqPerReq = atoi(argv[++i]);
            } else {
                exit_required("-r");
            }
        } else if (strcmp(argv[i], "-t") == 0) {
            if (i < argc - 1) {
                stbInfo->childTblCount = atoi(argv[++i]);
            } else {
                exit_required("-t");
            }
        } else if (strcmp(argv[i], "-n") == 0) {
            if (i < argc - 1) {
                stbInfo->insertRows = atol(argv[++i]);
            } else {
                exit_required("-n");
            }
        } else if (strcmp(argv[i], "-d") == 0) {
            if (i < argc - 1) {
                database->dbName = argv[++i];
            } else {
                exit_required("-d");
            }
        } else if (strcmp(argv[i], "-l") == 0) {
            if (i < argc - 1) {
                g_arguments->demo_mode = false;
                g_arguments->intColumnCount = atoi(argv[++i]);
            } else {
                exit_required("-l");
            }
        } else if (strcmp(argv[i] , "-A") == 0) {
            if (i < argc - 1) {
                g_arguments->demo_mode = false;
                parse_field_datatype(argv[++i], stbInfo->tags, true);
            } else {
                exit_required("-A");
            }
        } else if (strcmp(argv[i], "-b") == 0) {
            if (i < argc - 1) {
                g_arguments->demo_mode = false;
                parse_field_datatype(argv[++i], stbInfo->cols, false);
            } else {
                exit_required("-b");
            }
        } else if (strcmp(argv[i], "-w") == 0) {
            if (i < argc - 1) {
                g_arguments->binwidth = atoi(argv[++i]);
            } else {
                exit_required("-w");
            }
        } else if (strcmp(argv[i], "-m") == 0) {
            if (i < argc - 1) {
                stbInfo->childTblPrefix = argv[++i];
            } else {
                exit_required("-m");
            }
        } else if (strcmp(argv[i], "-E") == 0) {
            stbInfo->escape_character = true;
        } else if (strcmp(argv[i], "-C") == 0) {
            g_arguments->chinese = true;
        } else if (strcmp(argv[i], "-N") == 0) {
            g_arguments->demo_mode = false;
            stbInfo->use_metric = false;
            benchArrayClear(stbInfo->tags);
        } else if (strcmp(argv[i], "-M") == 0) {
            g_arguments->demo_mode = false;
        } else if (strcmp(argv[i], "-x") == 0) {
            g_arguments->aggr_func = true;
        } else if (strcmp(argv[i], "-y") == 0) {
            g_arguments->answer_yes = true;
        } else if (strcmp(argv[i], "-R") == 0) {
            if (i < argc - 1) {
                stbInfo->disorderRange = atoi(argv[++i]);
            } else {
                exit_required("-R");
            }
        } else if (strcmp(argv[i], "-O") == 0) {
            if (i < argc - 1) {
                stbInfo->disorderRatio = atoi(argv[++i]);
            } else {
                exit_required("-O");
            }
        } else if (strcmp(argv[i], "-a") == 0) {
            if (i < argc - 1) {
                database->dbCfg.replica = atoi(argv[++i]);
            } else {
                exit_required("-a");
            }
        } else if (strcmp(argv[i], "-g") == 0) {
            g_arguments->debug_print = true;
        } else if (strcmp(argv[i], "-G") == 0) {
            g_arguments->performance_print = true;
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-?") == 0) {
            printHelp();
            exit(EXIT_SUCCESS);
        } else if (strcmp(argv[i], "-V") == 0 || strcmp(argv[i], "--version") == 0) {
            fprintf(stdout, "%s\n", taos_get_client_info());
            exit(EXIT_SUCCESS);
        } else if (strcmp(argv[i], "--usage") == 0) {
            printUsage();
            exit(EXIT_SUCCESS);
        } else {
            errorPrint(stderr, "unknown option: %s\n", argv[i]);
            exit(EXIT_FAILURE);
        }
    }
}