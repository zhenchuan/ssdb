#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <string>
#include <vector>
#include "link.h"
#include "util/fde.h"
#include "util/log.h"
#include "version.h"

struct Data
{
	std::string key;
	std::string val;
	std::string num;
};

std::vector<Data *> *ds;
Fdevents *fdes;
std::vector<Link *> *free_links;
std::vector<Link *> *busy_links;


void welcome(){
	printf("ssdb-bench - SSDB benchmark tool, %s\n", SSDB_VERSION);
	printf("Copyright (c) 2013-2014 ideawu.com\n");
	printf("\n");
}

void usage(int argc, char **argv){
	printf("Usage:\n");
	printf("    %s [ip] [port] [requests] [clients]\n", argv[0]);
	printf("\n");
	printf("Options:\n");
	printf("    ip          server ip (default 127.0.0.1)\n");
	printf("    port        server port (default 8888)\n");
	printf("    requests    Total number of requests (default 10000)\n");
	printf("    clients     Number of parallel connections (default 50)\n");
	printf("\n");
}

void init_data(int num){
	srand(time(NULL));
	ds = new std::vector<Data *>();
	for(int i=0; i<num; i++){
		Data *d = new Data();
		char buf[1024];

		int n = rand();
		snprintf(buf, sizeof(buf), "%d", n);
		d->num = buf;
		snprintf(buf, sizeof(buf), "k%010d", n);
		d->key = buf;
		snprintf(buf, sizeof(buf), "v%0100d", n);
		d->val = buf;
		ds->push_back(d);
	}
}

void init_links(int num, const char *path){
	fdes = new Fdevents();
	free_links = new std::vector<Link *>();
	busy_links = new std::vector<Link *>();

	for(int i=0; i<num; i++){
		Link *link = Link::connect(path);
		if(!link){
			printf("connect error! %s\n", strerror(errno));
			exit(0);
		}
		fdes->set(link->fd(), FDEVENT_IN, 0, link);
		free_links->push_back(link);
	}
}

void init_links(int num, const char *ip, int port){
	fdes = new Fdevents();
	free_links = new std::vector<Link *>();
	busy_links = new std::vector<Link *>();

	for(int i=0; i<num; i++){
		Link *link = Link::connect(ip, port);
		if(!link){
			printf("connect error! %s\n", strerror(errno));
			exit(0);
		}
		fdes->set(link->fd(), FDEVENT_IN, 0, link);
		free_links->push_back(link);
	}
}

void bench(std::string cmd){
	int total = ds->size();
	int finished = 0;
	int num_sent = 0;
	
	printf("========== %s ==========\n", cmd.c_str());

	double stime = millitime();
	while(1){
		while(!free_links->empty() && num_sent < ds->size()){
			Link *link = free_links->back();
			free_links->pop_back();

			Data *d = ds->at(num_sent);
			num_sent ++;

			if(cmd == "set"){
				link->send(cmd, d->key, d->val);
			}else if(cmd == "get"){
				link->send(cmd, d->key);
			}else if(cmd == "del"){
				link->send(cmd, d->key);
			}else if(cmd == "hset"){
				link->send(cmd, "h", d->key, d->val);
			}else if(cmd == "hget"){
				link->send(cmd, "h", d->key);
			}else if(cmd == "hdel"){
				link->send(cmd, "h", d->key);
			}else if(cmd == "zset"){
				link->send(cmd, "z", d->key, d->num);
			}else if(cmd == "zget"){
				link->send(cmd, "z", d->key);
			}else if(cmd == "zdel"){
				link->send(cmd, "z", d->key);
			}else if(cmd == "qpush"){
				link->send(cmd, "h", d->key, d->val);
			}else if(cmd == "qpop"){
				link->send(cmd, "h", d->key);
			}else{
				printf("bad command!\n");
				exit(0);
			}
			link->flush();
		}

		const Fdevents::events_t *events;
		events = fdes->wait(50);
		if(events == NULL){
			log_fatal("events.wait error: %s", strerror(errno));
			break;
		}

		for(int i=0; i<(int)events->size(); i++){
			const Fdevent *fde = events->at(i);
			Link *link = (Link *)fde->data.ptr;

			int len = link->read();
			if(len <= 0){
				log_fatal("fd: %d, read: %d, delete link", link->fd(), len);
				exit(0);
			}

			const std::vector<Bytes> *resp = link->recv();
			if(resp == NULL){
				printf("error\n");
				break;
			}else if(resp->empty()){
				if(link->read() <= 0){
					printf("read end\n");
					break;
				}
				continue;
			}else{
				if(resp->at(0) != "ok"){
					log_error("bad response: %s", resp->at(0).String().c_str());
					exit(0);
				}
				free_links->push_back(link);
				finished ++;
				if(finished == total){
					double etime = millitime();
					double ts = (stime == etime)? 1 : (etime - stime);
					double speed = total / ts;
					printf("qps: %d, time: %.3f s\n", (int)speed, ts);
					return;
				}
			}
		}
	}
}

int main(int argc, char **argv){
	const char *ip = "127.0.0.1";
	int port = 8888;
	int requests = 10000;
	int clients = 50;

	welcome();
	for(int i=1; i<argc; i++){
		if(strcmp("-v", argv[i]) == 0){
			usage(argc, argv);
			exit(0);
		}
	}
	if(argc > 1){
		ip = argv[1];
	}
	if(argc > 2){
		port = atoi(argv[2]);
	}
	if(argc > 3){
		requests = atoi(argv[3]);
	}
	if(argc > 4){
		clients = atoi(argv[4]);
	}

	//printf("preparing data...\n");
	init_data(requests);
	//printf("preparing links...\n");
	if(port == 0){
		init_links(clients, ip);
	}else{
		init_links(clients, ip, port);
	}

	bench("set");
	bench("get");
	bench("del");

	bench("hset");
	bench("hget");
	bench("hdel");

	bench("zset");
	bench("zget");
	bench("zdel");

	bench("qpush");
	bench("qpop");
	
	printf("\n");

	return 0;
}

