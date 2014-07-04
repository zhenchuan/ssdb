#include "ssdb.h"
#include "slave.h"

#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"

#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"

SSDB::SSDB(){
	db = NULL;
	meta_db = NULL;
	expiry_db = NULL;
	binlogs = NULL;
}

SSDB::~SSDB(){
	for(std::vector<Slave *>::iterator it = slaves.begin(); it != slaves.end(); it++){
		Slave *slave = *it;
		slave->stop();
		delete slave;
	}
	if(binlogs){
		delete binlogs;
	}
	if(db){
		delete db;
	}
	if(options.block_cache){
		//delete options.block_cache; //shared_ptr<rocksdb::Cache>
	}
	if(options.filter_policy){
		delete options.filter_policy;
	}
	if(meta_db){
		delete meta_db;
	}
	if(expiry_db){
		delete expiry_db;
	}
	log_debug("SSDB finalized");
}

SSDB* SSDB::open(const Config &conf, const std::string &base_dir){
	std::string main_db_path = base_dir + "/data";
	std::string expiry_db_path = base_dir + "/expiry";
	std::string meta_db_path = base_dir + "/meta";
	size_t cache_size = (size_t)conf.get_num("rocksdb.cache_size");
	int write_buffer_size = conf.get_num("rocksdb.write_buffer_size");
	int block_size = conf.get_num("rocksdb.block_size");
	std::string disable_seek_compaction = conf.get_str("rocksdb.disable_seek_compaction");
	std::string compression = conf.get_str("rocksdb.compression");

	strtolower(&compression);
	if(compression != "yes"){
		compression = "no";
	}
	strtolower(&disable_seek_compaction);
	if(disable_seek_compaction!="no"){
		disable_seek_compaction = "yes";
	}

	if(cache_size <= 0){
		cache_size = 8;
	}
	if(write_buffer_size <= 0){
		write_buffer_size = 4;
	}
	if(block_size <= 0){
		block_size = 4;
	}

	log_info("main_db          : %s", main_db_path.c_str());
	log_info("meta_db          : %s", meta_db_path.c_str());
	log_info("expiry_db          : %s", expiry_db_path.c_str());
	log_info("cache_size       : %d MB", cache_size);
	log_info("block_size       : %d KB", block_size);
	log_info("write_buffer     : %d MB", write_buffer_size);
	log_info("disable_seek_compaction : %s", disable_seek_compaction.c_str());
	log_info("compression      : %s", compression.c_str());

	SSDB *ssdb = new SSDB();
	//
	auto env = rocksdb::Env::Default();
	env->SetBackgroundThreads(10,rocksdb::Env::LOW);
	env->SetBackgroundThreads(10,rocksdb::Env::HIGH);
	ssdb->options.env = env;
	ssdb->options.max_background_compactions = 10;
	ssdb->options.max_background_flushes = 10;//如果这里设置,就会用到高优先级的线程池,注意增加对应的高优先级的线程池的数量.
	//
	ssdb->options.create_if_missing = true;
	ssdb->options.filter_policy = rocksdb::NewBloomFilterPolicy(10);

	ssdb->options.block_cache = rocksdb::NewLRUCache(cache_size * 1024 * 1024);
	ssdb->options.block_size = block_size * 1024;

	//The idea is to keep level0_file_num_compaction_trigger * write_buffer_size = max_bytes_for_level_base to minimize write amplification.
	ssdb->options.write_buffer_size = 64 * 1024 * 1024;
	ssdb->options.max_write_buffer_number = 4;
	ssdb->options.min_write_buffer_number_to_merge = 2;
	//ssdb->options.compaction_speed = compaction_speed;

	//Options.target_file_size_base and Options.max_bytes_for_level_base are for L1.
	ssdb->options.target_file_size_base = 1024 * 1024 * 64;
	//ssdb->options.target_file_size_multiplier = 10;

	ssdb->options.inplace_update_support = false;

	ssdb->options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleLevel;//rocksdb::CompactionStyle::kCompactionStyleUniversal

	ssdb->options.level0_file_num_compaction_trigger = 4;
	ssdb->options.level0_stop_writes_trigger = 12;
	ssdb->options.level0_slowdown_writes_trigger = 8;

	//ssdb->options.bytes_per_sync = 4 * 1024 * 1024 ;

	//ssdb->options.allow_os_buffer = true;

	//ssdb->options.allow_mmap_writes = true;
	ssdb->options.statistics = rocksdb::CreateDBStatistics();
	if(compression == "yes"){
		ssdb->options.compression = rocksdb::kSnappyCompression;
	}else{
		ssdb->options.compression = rocksdb::kNoCompression;
	}

	if(disable_seek_compaction=="yes"){
		ssdb->options.disable_seek_compaction = true;
	}else{
		ssdb->options.disable_seek_compaction = false;
	}

	rocksdb::Status status;
	{
		rocksdb::Options options;
		options.create_if_missing = true;
		status = rocksdb::DB::Open(options, meta_db_path, &ssdb->meta_db);
		if(!status.ok()){
			goto err;
		}
	}

	{
		rocksdb::Options options;
		options.create_if_missing = true;
		options.write_buffer_size = 64 * 1024 * 1024;
		status = rocksdb::DB::Open(options,expiry_db_path,&ssdb->expiry_db);
		if(!status.ok()){
			goto err;
		}
	}

	status = rocksdb::DB::Open(ssdb->options, main_db_path, &ssdb->db);
	if(!status.ok()){
		log_error("open main_db failed");
		goto err;
	}

	ssdb->binlogs = new BinlogQueue(ssdb->db);

	{ // slaves
		const Config *repl_conf = conf.get("replication");
		if(repl_conf != NULL){
			std::vector<Config *> children = repl_conf->children;
			for(std::vector<Config *>::iterator it = children.begin(); it != children.end(); it++){
				Config *c = *it;
				if(c->key != "slaveof"){
					continue;
				}
				std::string ip = c->get_str("ip");
				int port = c->get_num("port");
				if(ip == "" || port <= 0 || port > 65535){
					continue;
				}
				bool is_mirror = false;
				std::string type = c->get_str("type");
				if(type == "mirror"){
					is_mirror = true;
				}else{
					type = "sync";
					is_mirror = false;
				}
				
				std::string id = c->get_str("id");
				
				log_info("slaveof: %s:%d, type: %s", ip.c_str(), port, type.c_str());
				Slave *slave = new Slave(ssdb, ssdb->meta_db, ip.c_str(), port, is_mirror);
				if(!id.empty()){
					slave->set_id(id);
				}
				slave->start();
				ssdb->slaves.push_back(slave);
			}
		}
	}

	return ssdb;
err:
	if(ssdb){
		delete ssdb;
	}
	return NULL;
}

Iterator* SSDB::iterator(const std::string &start, const std::string &end, uint64_t limit) const{
	rocksdb::Iterator *it;
	rocksdb::ReadOptions iterate_options;
	iterate_options.fill_cache = false;//??? filter是每个block一个还是每个sstable一个? | 每个sstable.
	iterate_options.tailing = false;
	it = db->NewIterator(iterate_options);
	it->Seek(start);
	if(it->Valid() && it->key() == start){
		it->Next();
	}
	return new Iterator(it, end, limit);
}

Iterator* SSDB::rev_iterator(const std::string &start, const std::string &end, uint64_t limit) const{
	rocksdb::Iterator *it;
	rocksdb::ReadOptions iterate_options;
	iterate_options.fill_cache = false;
	iterate_options.tailing = false;
	it = db->NewIterator(iterate_options);
	it->Seek(start);
	if(!it->Valid()){
		it->SeekToLast();
	}else{
		it->Prev();
	}
	return new Iterator(it, end, limit, Iterator::BACKWARD);
}


/* raw operates */

int64_t SSDB::expiry_get(const Bytes &key) const {
	std::string val;
	rocksdb::ReadOptions opts;
	opts.fill_cache = false;
	rocksdb::Status s = expiry_db->Get(opts, key.Slice(), &val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}
	int64_t ex = str_to_int64(val);
	return (ex - time_second());
}

int SSDB::expiry_set(const Bytes &key, const int64_t ttl) const{

	int64_t expired = time_second() + ttl ;
	char data[30];
	int size = snprintf(data, sizeof(data), "%" PRId64, expired);
	if(size <= 0){
		log_error("snprintf return error!");
		return -1;
	}

	rocksdb::WriteOptions write_opts;
	rocksdb::Status s = expiry_db->Put(write_opts, key.Slice(), Bytes(data, size).Slice());
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDB::raw_set(const Bytes &key, const Bytes &val) const{
	rocksdb::WriteOptions write_opts;
	rocksdb::Status s = db->Put(write_opts, key.Slice(), val.Slice());
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDB::raw_del(const Bytes &key) const{
	rocksdb::WriteOptions write_opts;
	rocksdb::Status s = db->Delete(write_opts, key.Slice());
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDB::raw_get(const Bytes &key, std::string *val) const{
	rocksdb::ReadOptions opts;
	opts.fill_cache = false;
	rocksdb::Status s = db->Get(opts, key.Slice(), val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

std::vector<std::string> SSDB::info() const{
	//  "rocksdb.num-files-at-level<N>" - return the number of files at level <N>,
	//     where <N> is an ASCII representation of a level number (e.g. "0").
	//  "rocksdb.stats" - returns a multi-line string that describes statistics
	//     about the internal operation of the DB.
	//  "rocksdb.sstables" - returns a multi-line string that describes all
	//     of the sstables that make up the db contents.
	std::vector<std::string> info;
	std::vector<std::string> keys;
	/*
	for(int i=0; i<7; i++){
		char buf[128];
		snprintf(buf, sizeof(buf), "rocksdb.num-files-at-level%d", i);
		keys.push_back(buf);
	}
	*/
	keys.push_back("rocksdb.stats");
	//keys.push_back("rocksdb.sstables");

	for(size_t i=0; i<keys.size(); i++){
		std::string key = keys[i];
		std::string val;
		if(db->GetProperty(key, &val)){
			info.push_back(key);
			info.push_back(val);
		}
	}

	return info;
}

void SSDB::compact() const{
	db->CompactRange(NULL, NULL);
}

int SSDB::key_range(std::vector<std::string> *keys) const{
	int ret = 0;
	std::string kstart, kend;
	std::string hstart, hend;
	std::string zstart, zend;
	
	Iterator *it;
	
	it = this->iterator(encode_kv_key(""), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::KV){
			std::string n;
			if(decode_kv_key(ks, &n) == -1){
				ret = -1;
			}else{
				kstart = n;
			}
		}
	}
	delete it;
	
	it = this->rev_iterator(encode_kv_key("\xff"), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::KV){
			std::string n;
			if(decode_kv_key(ks, &n) == -1){
				ret = -1;
			}else{
				kend = n;
			}
		}
	}
	delete it;
	
	it = this->iterator(encode_hsize_key(""), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::HSIZE){
			std::string n;
			if(decode_hsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				hstart = n;
			}
		}
	}
	delete it;
	
	it = this->rev_iterator(encode_hsize_key("\xff"), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::HSIZE){
			std::string n;
			if(decode_hsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				hend = n;
			}
		}
	}
	delete it;
	
	it = this->iterator(encode_zsize_key(""), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::ZSIZE){
			std::string n;
			if(decode_hsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				zstart = n;
			}
		}
	}
	delete it;
	
	it = this->rev_iterator(encode_zsize_key("\xff"), "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == DataType::ZSIZE){
			std::string n;
			if(decode_hsize_key(ks, &n) == -1){
				ret = -1;
			}else{
				zend = n;
			}
		}
	}
	delete it;

	keys->push_back(kstart);
	keys->push_back(kend);
	keys->push_back(hstart);
	keys->push_back(hend);
	keys->push_back(zstart);
	keys->push_back(zend);
	
	return ret;
}
