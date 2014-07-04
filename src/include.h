#ifndef SSDB_INCLUDE_H_
#define SSDB_INCLUDE_H_

#include <inttypes.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <math.h>
#include <fcntl.h>
#include <assert.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <atomic>

#include "version.h"

#if defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
#include <sys/resource.h>

	#if defined(__APPLE__) && defined(__MACH__)
	#include <mach/mach.h>
	#endif
#endif

#ifndef UINT64_MAX
	#define UINT64_MAX        18446744073709551615ULL
#endif


static const int SSDB_SCORE_WIDTH		= 9;
static const int SSDB_KEY_LEN_MAX		= 255;


static inline double millitime(){
	struct timeval now;
	gettimeofday(&now, NULL);
	double ret = now.tv_sec + now.tv_usec/1000.0/1000.0;
	return ret;
}

//Returns the peak (maximum so far) resident set size (physical memory use) measured in bytes,
//or zero if the value cannot be determined on this OS.
static inline size_t peak_rss(){
#if defined(__unix__) || defined(__unix) || defined(unix) || (defined(__APPLE__) && defined(__MACH__))
		/* BSD, Linux, and OSX -------------------------------------- */
		struct rusage rusage;
		getrusage( RUSAGE_SELF, &rusage );
		#if defined(__APPLE__) && defined(__MACH__)
			return (size_t)rusage.ru_maxrss;
		#else
			return (size_t)(rusage.ru_maxrss * 1024L);
		#endif
#else
	/* Unknown OS ----------------------------------------------- */
	return (size_t)0L;			/* Unsupported. */
#endif
}

//Returns the current resident set size (physical memory use) measured in bytes,
//or zero if the value cannot be determined on this OS.
static inline size_t current_rss( ){
	#if defined(__APPLE__) && defined(__MACH__)
		/* OSX ------------------------------------------------------ */
		struct mach_task_basic_info info;
		mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
		if ( task_info( mach_task_self( ), MACH_TASK_BASIC_INFO,
			(task_info_t)&info, &infoCount ) != KERN_SUCCESS )
			return (size_t)0L;		/* Can't access? */
		return (size_t)info.resident_size;

	#elif defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
		/* Linux ---------------------------------------------------- */
		long rss = 0L;
		FILE* fp = NULL;
		if ( (fp = fopen( "/proc/self/statm", "r" )) == NULL )
			return (size_t)0L;		/* Can't open? */
		if ( fscanf( fp, "%*s%ld", &rss ) != 1 )
		{
			fclose( fp );
			return (size_t)0L;		/* Can't read? */
		}
		fclose( fp );
		return (size_t)rss * (size_t)sysconf( _SC_PAGESIZE);

	#else
		/* AIX, BSD, Solaris, Windows and Unknown OS ------------------------ */
		return (size_t)0L;			/* Unsupported. */
	#endif
}

static inline int64_t time_ms(){
	struct timeval now;
	gettimeofday(&now, NULL);
	return now.tv_sec * 1000 + now.tv_usec/1000;
}

static inline int64_t time_second(){
	struct timeval now;
	gettimeofday(&now,NULL);
	return now.tv_sec;
}

class DataType{
public:
	static const char SYNCLOG	= 1;
	static const char KV		= 'k';
	static const char HASH		= 'h'; // hashmap(sorted by key)
	static const char HSIZE		= 'H';
	static const char ZSET		= 's'; // key => score
	static const char ZSCORE	= 'z'; // key|score => ""
	static const char ZSIZE		= 'Z';
	static const char QUEUE		= 'q';
	static const char QSIZE		= 'Q';
	static const char MIN_PREFIX = HASH;
	static const char MAX_PREFIX = ZSET;
};

class BinlogType{
public:
	static const char NOOP		= 0;
	static const char SYNC		= 1;
	static const char MIRROR	= 2;
	static const char COPY		= 3;
};

class BinlogCommand{
public:
	static const char NONE  = 0;
	static const char KSET  = 1;
	static const char KDEL  = 2;
	static const char HSET  = 3;
	static const char HDEL  = 4;
	static const char ZSET  = 5;
	static const char ZDEL  = 6;

	static const char QPUSH_BACK	= 10;
	static const char QPUSH_FRONT	= 11;
	static const char QPOP_BACK		= 12;
	static const char QPOP_FRONT	= 13;
	
	static const char BEGIN  = 7;
	static const char END    = 8;
};
	
#endif

