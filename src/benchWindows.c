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

void commandLineParseArgument(int argc, char *argv[]) {
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
                    g_arguments->db->superTbls->iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(mode, "stmt")) {
                    g_arguments->db->superTbls->iface = STMT_IFACE;
                } else if (0 == strcasecmp(mode, "rest")) {
                    g_arguments->db->superTbls->iface = REST_IFACE;
                } else if (0 == strcasecmp(mode, "sml")) {
                    g_arguments->db->superTbls->iface = SML_IFACE;
                } else {
                    errorPrint(stderr,
                               "Invalid -I: %s, will auto set to default (taosc)\n",
                               mode);
                    g_arguments->db->superTbls->iface = TAOSC_IFACE;
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
                g_arguments->db->superTbls->insert_interval = atoi(argv[++i]);
            } else {
                exit_required("-i");
            }
        } else if (strcmp(argv[i], "-S") == 0) {
            if (i < argc - 1) {
                g_arguments->db->superTbls->timestamp_step = atol(argv[++i]);
            } else {
                exit_required("-S");
            }
        } else if (strcmp(argv[i], "-B") == 0) {
            if (i < argc - 1) {
                g_arguments->db->superTbls->interlaceRows = atoi(argv[++i]);
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
                g_arguments->db->superTbls->childTblCount = atoi(argv[++i]);
            } else {
                exit_required("-t");
            }
        } else if (strcmp(argv[i], "-n") == 0) {
            if (i < argc - 1) {
                g_arguments->db->superTbls->insertRows = atol(argv[++i]);
            } else {
                exit_required("-n");
            }
        } else if (strcmp(argv[i], "-d") == 0) {
            if (i < argc - 1) {
                g_arguments->db->dbName = argv[++i];
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
                char* tags = argv[++i];
                count_datatype(tags, &(g_arguments->db->superTbls->tagCount));
                tmfree(g_arguments->db->superTbls->tags);
                g_arguments->db->superTbls->tags =
                        calloc(g_arguments->db->superTbls->tagCount, sizeof(Column));
                if (g_arguments->db->superTbls->tags == NULL) {
                    errorPrint(stderr, "%s", "memory allocation failed\n");
                    exit(EXIT_FAILURE);
                }
                g_memoryUsage += g_arguments->db->superTbls->tagCount * sizeof(Column);
                if (parse_tag_datatype(tags, g_arguments->db->superTbls->tags)) {
                    tmfree(g_arguments->db->superTbls->tags);
                    exit(EXIT_FAILURE);
                }
            } else {
                exit_required("-A");
            }
        } else if (strcmp(argv[i], "-b") == 0) {
            if (i < argc - 1) {
                g_arguments->demo_mode = false;
                char* cols = argv[++i];
                tmfree(g_arguments->db->superTbls->columns);
                count_datatype(cols, &(g_arguments->db->superTbls->columnCount));
                g_arguments->db->superTbls->columns =
                        calloc(g_arguments->db->superTbls->columnCount, sizeof(Column));
                if (g_arguments->db->superTbls->columns == NULL) {
                    errorPrint(stderr, "%s", "memory allocation failed\n");
                    exit(EXIT_FAILURE);
                }
                g_memoryUsage += g_arguments->db->superTbls->columnCount * sizeof(Column);
                if (parse_col_datatype(cols, g_arguments->db->superTbls->columns)) {
                    tmfree(g_arguments->db->superTbls->columns);
                    exit(EXIT_FAILURE);
                }
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
                g_arguments->db->superTbls->childTblPrefix = argv[++i];
            } else {
                exit_required("-m");
            }
        } else if (strcmp(argv[i], "-E") == 0) {
            g_arguments->db->superTbls->escape_character = true;
        } else if (strcmp(argv[i], "-C") == 0) {
            g_arguments->chinese = true;
        } else if (strcmp(argv[i], "-N") == 0) {
            g_arguments->demo_mode = false;
            g_arguments->db->superTbls->use_metric = false;
            g_arguments->db->superTbls->tagCount = 0;
        } else if (strcmp(argv[i], "-M") == 0) {
            g_arguments->demo_mode = false;
        } else if (strcmp(argv[i], "-x") == 0) {
            g_arguments->aggr_func = true;
        } else if (strcmp(argv[i], "-y") == 0) {
            g_arguments->answer_yes = true;
        } else if (strcmp(argv[i], "-R") == 0) {
            if (i < argc - 1) {
                g_arguments->db->superTbls->disorderRange = atoi(argv[++i]);
            } else {
                exit_required("-R");
            }
        } else if (strcmp(argv[i], "-O") == 0) {
            if (i < argc - 1) {
                g_arguments->db->superTbls->disorderRatio = atoi(argv[++i]);
            } else {
                exit_required("-O");
            }
        } else if (strcmp(argv[i], "-a") == 0) {
            if (i < argc - 1) {
                g_arguments->db->dbCfg.replica = atoi(argv[++i]);
            } else {
                exit_required("-a");
            }
        } else if (strcmp(argv[i], "-g") == 0) {
            g_arguments->debug_print = true;
        } else if (strcmp(argv[i], "-G") == 0) {
            g_arguments->performance_print = true;
        } else {
            errorPrint(stderr, "unknown option: %s\n", argv[i]);
            exit(EXIT_FAILURE);
        }
    }
}