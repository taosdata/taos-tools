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
int32_t bench_parse_args_no_argp(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-V") == 0 || strcmp(argv[i], "--version") == 0) {
            printVersion();
            exit(EXIT_SUCCESS);
        }

        if(strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "--usage") == 0
            || strcmp(argv[i], "-?") == 0) {
            bench_print_help();
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

        if (key[1] == 'f' || key[1] == 'c'
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
            bench_parse_single_opt(key[1], val);
            i++;
        } else if (key[1] == 'E' || key[1] == 'C'
                || key[1] == 'N' || key[1] == 'M'
                || key[1] == 'x' || key[1] == 'y'
                || key[1] == 'g' || key[1] == 'G' || key[1] == 'V') {
            bench_parse_single_opt(key[1], NULL);
        } else {
            errorPrint("Invalid option %s\r\n", key);
            return -1;
        }
    }
    return 0;
}
#endif  // LINUX

