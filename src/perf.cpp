#include "perf.h"
#include <atomic>
#include <stdio.h>

static std::atomic<unsigned long> NUM_GETS(ATOMIC_VAR_INIT(0UL));
static std::atomic<unsigned long> NUM_EMPTY_GETS(ATOMIC_VAR_INIT(0UL));
static std::atomic<unsigned long> NUM_SETS(ATOMIC_VAR_INIT(0UL));

void perf::incrGets(void) throw(){
	NUM_GETS.fetch_add(1);
}
void perf::incrEmptyGets(void) throw(){
	NUM_EMPTY_GETS.fetch_add(1);
}
void perf::incrSets(void) throw(){
	NUM_SETS.fetch_add(1);
}

void perf::report(char *buf) throw(){
	sprintf(buf,"gets: %lu\nempty.gets: %lu\nsets: %lu",
			NUM_GETS.load(),NUM_EMPTY_GETS.load(),NUM_SETS.load());

}


