/*
 * =====================================================================================
 *
 *       Filename:  benchSys.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2022年12月17日 21时40分29秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */
#include <stdlib.h>
#include <bench.h>

#ifndef LINUX
void benchPrintHelp() {
    char indent[] = "  ";
    printf("Usage: taosBenchmark [OPTION ...] \r\n\r\n");
    printf("%s%s%s%s\r\n", indent, "-f,", indent, BENCH_FILE);
    printf("%s%s%s%s\r\n", indent, "-a,", indent, BENCH_REPLICA);
    printf("%s%s%s%s\r\n", indent, "-A,", indent, BENCH_TAGS);
    printf("%s%s%s%s\r\n", indent, "-b,", indent, BENCH_COLS);
    printf("%s%s%s%s\r\n", indent, "-B,", indent, BENCH_INTERLACE);
    printf("%s%s%s%s\r\n", indent, "-c,", indent, BENCH_CFG_DIR);
    printf("%s%s%s%s\r\n", indent, "-C,", indent, BENCH_CHINESE);
    printf("%s%s%s%s\r\n", indent, "-d,", indent, BENCH_DATABASE);
    printf("%s%s%s%s\r\n", indent, "-E,", indent, BENCH_ESCAPE);
    printf("%s%s%s%s\r\n", indent, "-F,", indent, BENCH_PREPARE);
    printf("%s%s%s%s\r\n", indent, "-g,", indent, BENCH_DEBUG);
    printf("%s%s%s%s\r\n", indent, "-G,", indent, BENCH_PERFORMANCE);
    printf("%s%s%s%s\r\n", indent, "-h,", indent, BENCH_HOST);
    printf("%s%s%s%s\r\n", indent, "-i,", indent, BENCH_INTERVAL);
    printf("%s%s%s%s\r\n", indent, "-I,", indent, BENCH_MODE);
    printf("%s%s%s%s\r\n", indent, "-l,", indent, BENCH_COLS_NUM);
    printf("%s%s%s%s\r\n", indent, "-L,", indent, BENCH_PARTIAL_COL_NUM);
    printf("%s%s%s%s\r\n", indent, "-m,", indent, BENCH_PREFIX);
    printf("%s%s%s%s\r\n", indent, "-M,", indent, BENCH_RANDOM);
    printf("%s%s%s%s\r\n", indent, "-n,", indent, BENCH_ROWS);
    printf("%s%s%s%s\r\n", indent, "-N,", indent, BENCH_NORMAL);
    printf("%s%s%s%s\r\n", indent, "-k,", indent, BENCH_KEEPTRYING);
    printf("%s%s%s%s\r\n", indent, "-o,", indent, BENCH_OUTPUT);
    printf("%s%s%s%s\r\n", indent, "-O,", indent, BENCH_DISORDER);
    printf("%s%s%s%s\r\n", indent, "-p,", indent, BENCH_PASS);
    printf("%s%s%s%s\r\n", indent, "-P,", indent, BENCH_PORT);
    printf("%s%s%s%s\r\n", indent, "-r,", indent, BENCH_BATCH);
    printf("%s%s%s%s\r\n", indent, "-R,", indent, BENCH_RANGE);
    printf("%s%s%s%s\r\n", indent, "-S,", indent, BENCH_STEP);
    printf("%s%s%s%s\r\n", indent, "-s,", indent, BENCH_SUPPLEMENT);
    printf("%s%s%s%s\r\n", indent, "-t,", indent, BENCH_TABLE);
    printf("%s%s%s%s\r\n", indent, "-T,", indent, BENCH_THREAD);
    printf("%s%s%s%s\r\n", indent, "-u,", indent, BENCH_USER);
    printf("%s%s%s%s\r\n", indent, "-U,", indent, BENCH_SUPPLEMENT);
    printf("%s%s%s%s\r\n", indent, "-w,", indent, BENCH_WIDTH);
    printf("%s%s%s%s\r\n", indent, "-x,", indent, BENCH_AGGR);
    printf("%s%s%s%s\r\n", indent, "-y,", indent, BENCH_YES);
    printf("%s%s%s%s\r\n", indent, "-z,", indent, BENCH_TRYING_INTERVAL);
#ifdef WEBSOCKET
    printf("%s%s%s%s\r\n", indent, "-W,", indent, BENCH_DSN);
    printf("%s%s%s%s\r\n", indent, "-D,", indent, BENCH_TIMEOUT);
#endif
#ifdef TD_VER_COMPATIBLE_3_0_0_0
    printf("%s%s%s%s\r\n", indent, "-v,", indent, BENCH_VGROUPS);
#endif
    printf("%s%s%s%s\r\n", indent, "-V,", indent, BENCH_VERSION);
    printf("\r\n\r\nReport bugs to %s.\r\n", BENCH_EMAIL);
}

int32_t benchParseArgsNoArgp(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-V") == 0 || strcmp(argv[i], "--version") == 0) {
            printVersion();
            exit(EXIT_SUCCESS);
        }

        if(strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "--usage") == 0
            || strcmp(argv[i], "-?") == 0) {
            benchPrintHelp();
            exit(EXIT_SUCCESS);
        }

        char* key = argv[i];
        int32_t key_len = strlen(key);
        if (key_len != 2) {
            errorPrint("Invalid option %s\r\n", key);
            return -1;
        }
        if (key[0] != '-') {
            errorPrint("Invalid option %s\r\n", key);
            return -1;
        }

        if (   key[1] == 'f' || key[1] == 'c'
            || key[1] == 'h' || key[1] == 'P'
            || key[1] == 'I' || key[1] == 'u'
            || key[1] == 'p' || key[1] == 'o'
            || key[1] == 'T' || key[1] == 'i'
            || key[1] == 'S' || key[1] == 'B'
            || key[1] == 'r' || key[1] == 't'
            || key[1] == 'n' || key[1] == 'L'
            || key[1] == 'd' || key[1] == 'l'
            || key[1] == 'A' || key[1] == 'b'
            || key[1] == 'w' || key[1] == 'm'
            || key[1] == 'R' || key[1] == 'O'
            || key[1] == 'a' || key[1] == 'F'
            || key[1] == 'k' || key[1] == 'z'
#ifdef WEBSOCKET
            || key[1] == 'D' || key[1] == 'W'
#endif
#ifdef TD_VER_COMPATIBLE_3_0_0_0
            || key[1] == 'v'
#endif
        ) {
            if (i + 1 >= argc) {
                errorPrint("option %s requires an argument\r\n", key);
                return -1;
            }
            char* val = argv[i+1];
            if (val[0] == '-') {
                errorPrint("option %s requires an argument\r\n", key);
                return -1;
            }
            benchParseSingleOpt(key[1], val);
            i++;
        } else if (key[1] == 'E' || key[1] == 'C'
                || key[1] == 'N' || key[1] == 'M'
                || key[1] == 'x' || key[1] == 'y'
                || key[1] == 'g' || key[1] == 'G'
                || key[1] == 'V') {
            benchParseSingleOpt(key[1], NULL);
        } else {
            errorPrint("Invalid option %s\r\n", key);
            return -1;
        }
    }
    return 0;
}
#endif  // LINUX

