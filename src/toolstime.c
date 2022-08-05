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

#define _BSD_SOURCE

#ifdef DARWIN
#define _XOPEN_SOURCE
#else
#define _XOPEN_SOURCE 500
#endif

#define _DEFAULT_SOURCE

#ifdef LINUX
#include <unistd.h>
#include <strings.h>
#include <sys/time.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <inttypes.h>
#include <stdbool.h>
#include <time.h>


#include "bench.h"
#include "toolsdef.h"

static int32_t parseLocaltime(char* timestr, int64_t* time, int32_t timePrec, char delim);
static int32_t parseLocaltimeWithDst(char* timestr, int64_t* time, int32_t timePrec, char delim);

static int32_t (*parseLocaltimeFp[]) (char* timestr, int64_t* time, int32_t timePrec, char delim) = {
  parseLocaltime,
  parseLocaltimeWithDst
};

#ifdef LINUX
char *strnchr(char *haystack, char needle, int32_t len, bool skipquote) {
  for (int32_t i = 0; i < len; ++i) {

    // skip the needle in quote, jump to the end of quoted string
    if (skipquote && (haystack[i] == '\'' || haystack[i] == '"')) {
      char quote = haystack[i++];
      while(i < len && haystack[i++] != quote);
      if (i >= len) {
        return NULL;
      }
    }

    if (haystack[i] == needle) {
      return &haystack[i];
    }
  }

  return NULL;
}
#endif

char* forwardToTimeStringEnd(char* str) {
  int32_t i = 0;
  int32_t numOfSep = 0;

  while (str[i] != 0 && numOfSep < 2) {
    if (str[i++] == ':') {
      numOfSep++;
    }
  }

  while (str[i] >= '0' && str[i] <= '9') {
    i++;
  }

  return &str[i];
}
#ifdef LINUX
int64_t strnatoi(char *num, int32_t len) {
  int64_t ret = 0, i, dig, base = 1;

  if (len > (int32_t)strlen(num)) {
    len = (int32_t)strlen(num);
  }

  if ((len > 2) && (num[0] == '0') && ((num[1] == 'x') || (num[1] == 'X'))) {
    for (i = len - 1; i >= 2; --i, base *= 16) {
      if (num[i] >= '0' && num[i] <= '9') {
        dig = (num[i] - '0');
      } else if (num[i] >= 'a' && num[i] <= 'f') {
        dig = num[i] - 'a' + 10;
      } else if (num[i] >= 'A' && num[i] <= 'F') {
        dig = num[i] - 'A' + 10;
      } else {
        return 0;
      }
      ret += dig * base;
    }
  } else {
    for (i = len - 1; i >= 0; --i, base *= 10) {
      if (num[i] >= '0' && num[i] <= '9') {
        dig = (num[i] - '0');
      } else {
        return 0;
      }
      ret += dig * base;
    }
  }

  return ret;
}
#endif

int64_t parseFraction(char* str, char** end, int32_t timePrec) {
  int32_t i = 0;
  int64_t fraction = 0;

  const int32_t MILLI_SEC_FRACTION_LEN = 3;
  const int32_t MICRO_SEC_FRACTION_LEN = 6;
  const int32_t NANO_SEC_FRACTION_LEN = 9;

  int32_t factor[9] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000};
  int32_t times = 1;

  while (str[i] >= '0' && str[i] <= '9') {
    i++;
  }

  int32_t totalLen = i;
  if (totalLen <= 0) {
    return -1;
  }

  /* parse the fraction */
  if (timePrec == TSDB_TIME_PRECISION_MILLI) {
    /* only use the initial 3 bits */
    if (i >= MILLI_SEC_FRACTION_LEN) {
      i = MILLI_SEC_FRACTION_LEN;
    }

    times = MILLI_SEC_FRACTION_LEN - i;
  } else if (timePrec == TSDB_TIME_PRECISION_MICRO) {
    if (i >= MICRO_SEC_FRACTION_LEN) {
      i = MICRO_SEC_FRACTION_LEN;
    }
    times = MICRO_SEC_FRACTION_LEN - i;
  } else {
    assert(timePrec == TSDB_TIME_PRECISION_NANO);
    if (i >= NANO_SEC_FRACTION_LEN) {
      i = NANO_SEC_FRACTION_LEN;
    }
    times = NANO_SEC_FRACTION_LEN - i;
  }

  fraction = strnatoi(str, i) * factor[times];
  *end = str + totalLen;

  return fraction;
}
int64_t tools_user_mktime64(const unsigned int year0, const unsigned int mon0,
		const unsigned int day, const unsigned int hour,
		const unsigned int min, const unsigned int sec, int64_t time_zone)
{
  unsigned int mon = mon0, year = year0;

  /* 1..12 -> 11,12,1..10 */
  if (0 >= (int) (mon -= 2)) {
    mon += 12;	/* Puts Feb last since it has leap day */
    year -= 1;
  }

  //int64_t res = (((((int64_t) (year/4 - year/100 + year/400 + 367*mon/12 + day) +
  //               year*365 - 719499)*24 + hour)*60 + min)*60 + sec);
  int64_t res;
  res  = 367*((int64_t)mon)/12;
  res += year/4 - year/100 + year/400 + day + ((int64_t)year)*365 - 719499;
  res  = res*24;
  res  = ((res + hour) * 60 + min) * 60 + sec;

  return (res + time_zone);
}


#if defined(LINUX) || defined(DARWIN)
int32_t parseTimezone(char* str, int64_t* tzOffset) {
  int64_t hour = 0;

  int32_t i = 0;
  if (str[i] != '+' && str[i] != '-') {
    return -1;
  }

  i++;

  char* sep = strchr(&str[i], ':');
  if (sep != NULL) {
    int32_t len = (int32_t)(sep - &str[i]);

    hour = strnatoi(&str[i], len);
    i += len + 1;
  } else {
    hour = strnatoi(&str[i], 2);
    i += 2;
  }

  //return error if there're illegal charaters after min(2 Digits)
  char *minStr = &str[i];
  if (minStr[1] != '\0' && minStr[2] != '\0') {
      return -1;
  }


  int64_t minute = strnatoi(&str[i], 2);
  if (minute > 59) {
    return -1;
  }

  if (str[0] == '+') {
    *tzOffset = -(hour * 3600 + minute * 60);
  } else {
    *tzOffset = hour * 3600 + minute * 60;
  }

  return 0;
}
#endif

int32_t parseTimeWithTz(char* timestr, int64_t* time, int32_t timePrec, char delim) {

  int64_t factor = (timePrec == TSDB_TIME_PRECISION_MILLI) ? 1000 :
                             (timePrec == TSDB_TIME_PRECISION_MICRO ? 1000000 : 1000000000);
  int64_t tzOffset = 0;

  struct tm tm = {0};

  char* str;
  if (delim == 'T') {
    str = toolsStrpTime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
  } else if (delim == 0) {
    str = toolsStrpTime(timestr, "%Y-%m-%d %H:%M:%S", &tm);
  } else {
    str = NULL;
  }

  if (str == NULL) {
    return -1;
  }

/* mktime will be affected by TZ, set by using taos_options */
#ifdef WINDOWS
  int64_t seconds = tools_user_mktime64(tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, 0);
  //int64_t seconds = gmtime(&tm);
#else
  int64_t seconds = timegm(&tm);
#endif

  int64_t fraction = 0;
  str = forwardToTimeStringEnd(timestr);

  if ((str[0] == 'Z' || str[0] == 'z') && str[1] == '\0') {
    /* utc time, no millisecond, return directly*/
    *time = seconds * factor;
  } else if (str[0] == '.') {
    str += 1;
    if ((fraction = parseFraction(str, &str, timePrec)) < 0) {
      return -1;
    }

    *time = seconds * factor + fraction;

    char seg = str[0];
    if (seg != 'Z' && seg != 'z' && seg != '+' && seg != '-') {
      return -1;
    } else if ((seg == 'Z' || seg == 'z') && str[1] != '\0') {
      return -1;
    } else if (seg == '+' || seg == '-') {
      // parse the timezone
      if (parseTimezone(str, &tzOffset) == -1) {
        return -1;
      }

      *time += tzOffset * factor;
    }

  } else if (str[0] == '+' || str[0] == '-') {
    *time = seconds * factor + fraction;

    // parse the timezone
    if (parseTimezone(str, &tzOffset) == -1) {
      return -1;
    }

    *time += tzOffset * factor;
  } else {
    return -1;
  }

  return 0;
}

bool checkTzPresent(char *str, int32_t len) {
  char *seg = forwardToTimeStringEnd(str);
  int32_t seg_len = len - (int32_t)(seg - str);

  char *c = &seg[seg_len - 1];
  for (int i = 0; i < seg_len; ++i) {
    if (*c == 'Z' || *c  == 'z' || *c == '+' || *c == '-') {
      return true;
    }
    c--;
  }
  return false;
}

int32_t parseLocaltime(char* timestr, int64_t* time, int32_t timePrec, char delim) {
  *time = 0;
  struct tm tm = {0};

  char* str;
  if (delim == 'T') {
    str = toolsStrpTime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
  } else if (delim == 0) {
    str = toolsStrpTime(timestr, "%Y-%m-%d %H:%M:%S", &tm);
  } else {
    str = NULL;
  }

  if (str == NULL) {
    return -1;
  }

#ifdef _MSC_VER
#if _MSC_VER >= 1900
  int64_t timezone = _timezone;
#endif
#endif

  int64_t seconds = tools_user_mktime64(tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, timezone);

  int64_t fraction = 0;

  if (*str == '.') {
    /* parse the second fraction part */
    if ((fraction = parseFraction(str + 1, &str, timePrec)) < 0) {
      return -1;
    }
  }

  int64_t factor = (timePrec == TSDB_TIME_PRECISION_MILLI) ? 1000 :
                   (timePrec == TSDB_TIME_PRECISION_MICRO ? 1000000 : 1000000000);
  *time = factor * seconds + fraction;

  return 0;
}

int32_t parseLocaltimeWithDst(char* timestr, int64_t* time, int32_t timePrec, char delim) {
  *time = 0;
  struct tm tm = {0};
  tm.tm_isdst = -1;

  char* str;
  if (delim == 'T') {
    str = toolsStrpTime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
  } else if (delim == 0) {
    str = toolsStrpTime(timestr, "%Y-%m-%d %H:%M:%S", &tm);
  } else {
    str = NULL;
  }

  if (str == NULL) {
    return -1;
  }

  /* mktime will be affected by TZ, set by using taos_options */
  int64_t seconds = mktime(&tm);

  int64_t fraction = 0;

  if (*str == '.') {
    /* parse the second fraction part */
    if ((fraction = parseFraction(str + 1, &str, timePrec)) < 0) {
      return -1;
    }
  }

  int64_t factor = (timePrec == TSDB_TIME_PRECISION_MILLI) ? 1000 :
                   (timePrec == TSDB_TIME_PRECISION_MICRO ? 1000000 : 1000000000);
  *time = factor * seconds + fraction;
  return 0;
}

int32_t toolsParseTime(char* timestr, int64_t* time, int32_t len, int32_t timePrec, int8_t day_light) {
  /* parse datatime string in with tz */
  if (strnchr(timestr, 'T', len, false) != NULL) {
    if (checkTzPresent(timestr, len)) {
      return parseTimeWithTz(timestr, time, timePrec, 'T');
    } else {
      return (*parseLocaltimeFp[day_light])(timestr, time, timePrec, 'T');
    }
  } else {
    if (checkTzPresent(timestr, len)) {
      return parseTimeWithTz(timestr, time, timePrec, 0);
    } else {
      return (*parseLocaltimeFp[day_light])(timestr, time, timePrec, 0);
    }
  }
}

struct tm* toolsLocalTime(const time_t *timep, struct tm *result) {
#if defined(LINUX) || defined(DARWIN)
    localtime_r(timep, result);
#else
    localtime_s(result, timep);
#endif
    return result;
}
