/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <stdint.h>
#include <assert.h>

#ifdef WINDOWS
char *strsep(char **stringp, const char *delim) {
    char       *s;
    const char *spanp;
    int32_t     c, sc;
    char       *tok;
    if ((s = *stringp) == NULL) return (NULL);
    for (tok = s;;) {
        c = *s++;
        spanp = delim;
        do {
            if ((sc = *spanp++) == c) {
                if (c == 0)
                    s = NULL;
                else
                    s[-1] = 0;
                *stringp = s;
                return (tok);
            }
        } while (sc != 0);
    }
    /* NOTREACHED */
}

/* Copy no more than N characters of SRC to DEST, returning the address of
   the terminating '\0' in DEST, if any, or else DEST + N.  */
char *stpncpy(char *dest, const char *src, int n) {
    size_t size = strnlen(src, n);
    memcpy(dest, src, size);
    dest += size;
    if (size == n) return dest;
    return memset(dest, '\0', n - size);
}

char *stpcpy(char *dest, const char *src) {
    strcpy(dest, src);
    return dest + strlen(src);
}

void toolsLibFuncInclude() {
    assert(0);
    fread(0,0,0,0);
    srand(0);
    rand();
    realloc(0, 0);
    strtod(0, 0);
    fputs(0, 0);
}
#endif

