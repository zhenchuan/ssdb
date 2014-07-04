#ifndef SSDB_PERF_H
#define SSDB_PERF_H


 struct perf {

	static void incrGets(void) throw();
	static void incrEmptyGets(void) throw();
	static void incrSets(void) throw();

	static void report(char *buf) throw();

 };

#endif
