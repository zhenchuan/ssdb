#include "t_kv.h"
#include "util/strings.h"
#include "rocksdb/write_batch.h"
#include "perf.h"

int SSDB::multi_set(const std::vector<Bytes> &kvs, int offset, char log_type){
	Transaction trans(binlogs);

	std::vector<Bytes>::const_iterator it;
	it = kvs.begin() + offset;
	for(; it != kvs.end(); it += 2){
		const Bytes &key = *it;
		if(key.empty()){
			log_error("empty key!");
			return 0;
			//return -1;
		}
		const Bytes &val = *(it + 1);
		std::string buf = encode_kv_key(key);
		binlogs->Put(buf, val.Slice());
		binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	}
	rocksdb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("multi_set error: %s", s.ToString().c_str());
		return -1;
	}
	return (kvs.size() - offset)/2;
}

int SSDB::multi_del(const std::vector<Bytes> &keys, int offset, char log_type){
	Transaction trans(binlogs);

	std::vector<Bytes>::const_iterator it;
	it = keys.begin() + offset;
	for(; it != keys.end(); it++){
		const Bytes &key = *it;
		std::string buf = encode_kv_key(key);
		binlogs->Delete(buf);
		binlogs->add_log(log_type, BinlogCommand::KDEL, buf);
	}
	rocksdb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("multi_del error: %s", s.ToString().c_str());
		return -1;
	}
	return keys.size() - offset;
}

int SSDB::set(const Bytes &key, const Bytes &val, char log_type){
	if(key.empty()){
		log_error("empty key!");
		//return -1;
		return 0;
	}
	perf::incrSets();
	Transaction trans(binlogs);

	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, val.Slice());
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	rocksdb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDB::setnx(const Bytes &key, const Bytes &val, char log_type){
	if(key.empty()){
		log_error("empty key!");
		//return -1;
		return 0;
	}
	Transaction trans(binlogs);

	std::string tmp;
	int found = this->get(key, &tmp);
	if(found != 0){
		return 0;
	}
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, val.Slice());
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	rocksdb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDB::getset(const Bytes &key, std::string *val, const Bytes &newval, char log_type){
	if(key.empty()){
		log_error("empty key!");
		//return -1;
		return 0;
	}
	Transaction trans(binlogs);

	int found = this->get(key, val);
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, newval.Slice());
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	rocksdb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return found;
}


int SSDB::del(const Bytes &key, char log_type){
	Transaction trans(binlogs);

	std::string buf = encode_kv_key(key);
	binlogs->begin();
	binlogs->Delete(buf);
	binlogs->add_log(log_type, BinlogCommand::KDEL, buf);
	rocksdb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDB::incr(const Bytes &key, int64_t by, std::string *new_val, char log_type){
	Transaction trans(binlogs);

	int64_t val;
	std::string old;
	int ret = this->get(key, &old);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		val = by;
	}else{
		val = str_to_int64(old.data(), old.size()) + by;
	}

	*new_val = int64_to_str(val);
	std::string buf = encode_kv_key(key);
	
	binlogs->Put(buf, *new_val);
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);

	rocksdb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}
//kv结构的Get默认的fill_cache=true
int SSDB::get(const Bytes &key, std::string *val) const{
	perf::incrGets();
	std::string buf = encode_kv_key(key);
	rocksdb::Status s = db->Get(rocksdb::ReadOptions(), buf, val);
	if(s.IsNotFound()){
		perf::incrEmptyGets();
		return 0;
	}
	if(!s.ok()){
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

KIterator* SSDB::scan(const Bytes &start, const Bytes &end, uint64_t limit) const{
	std::string key_start, key_end;
	key_start = encode_kv_key(start);
	if(end.empty()){
		key_end = "";
	}else{
		key_end = encode_kv_key(end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->iterator(key_start, key_end, limit));
}

KIterator* SSDB::rscan(const Bytes &start, const Bytes &end, uint64_t limit) const{
	std::string key_start, key_end;

	key_start = encode_kv_key(start);
	if(start.empty()){
		key_start.append(1, 255);
	}
	if(end.empty()){
		key_end = "";
	}else{
		key_end = encode_kv_key(end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->rev_iterator(key_start, key_end, limit));
}


//ipinyou zset
int SSDB::zset_set(const Bytes &key, const Bytes &value, char log_type){
	return this->set(key,value);
}

int SSDB::zset_range(const Bytes &key,const uint64_t score,const uint64_t limit,std::vector<std::string>& result) {
	std::string old;
	int ret = this->get(key,&old);
	if(ret>0){
		if(score > 0){//按score过滤
			Decoder decoder(old.data(),old.size());
			uint64_t val;
			int i =0 ,j = 0;
			while((decoder.read_uint64(&val))!=-1){//读出的是little_endian ?
				uint64_t v = big_endian(val) >> 32;
				if(v < score || ++j > limit){
					break;
				}
				i ++ ;
			}
			int offset = i*sizeof(uint64_t) ;
			result.push_back(old.substr(0,offset));
		}else{
			int offset = limit * sizeof(uint64_t);
			if(old.size() <= offset){
				result.push_back(old);
			}else{
				result.push_back(old.substr(0,offset));
			}
		}
		return 1;
	}else{
		return 0;
	}
}

int SSDB::zset_incr(const Bytes &key,const Bytes &by,std::string *new_value,char log_type) {
	Transaction trans(binlogs);
	std::string old;
	std::string value_;
	int ret = this->get(key,&old);
	int total_size = 0;

	if(ret == -1 ){//获得key对应的value出错.
		return -1;
	}else if(ret == 0){//key不存在value值.
		value_ = by.String();
		total_size = by.size()/sizeof(uint64_t);
	}else{
		Decoder decoder_old(old.data(),old.size());
		Decoder decoder_incrs(by.data(),by.size());
		std::vector<uint64_t> olds = convert_chars_to_longs(decoder_old);
		std::vector<uint64_t> incres = convert_chars_to_longs(decoder_incrs);

		int size_i = incres.size(),size_j = olds.size();
		for(int i =0 ; i<size_i;i++){
			uint64_t encode_i = incres.at(i);
			uint64_t v = encode_i >> 32;
			uint64_t k = encode_i & 0xFFFFFFFF;
			bool found = false;

			for(int j =0 ;j<size_j ;j++){
				uint64_t encode_j = olds.at(j);
				uint64_t vv = (encode_j >> 32);
				uint64_t kk = (encode_j & 0xFFFFFFFF);

				if(k == kk){
					found = true;
					olds.at(j) = (((uint64_t)(v + vv)) << 32 | k);
					break;
				}
			}
			if(found==false){
				olds.push_back(encode_i);
			}

		}
		//TODO 更高效的排序方式.因为这里部分是排序的..
		std::sort(olds.begin(),olds.end(),std::greater<uint64_t>());

		for(int j=0;j<olds.size();j++){
			char encoded[sizeof(uint64_t)];
			encode_uint64(olds.at(j),encoded);
			value_.append(encoded,sizeof(encoded));
		}

		total_size = olds.size();
	}

	*new_value = int64_to_str(total_size);
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf,value_);//转化为kv操作存储.
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);

	rocksdb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("incr error: %s", s.ToString().c_str());
		return -1;
	}

	return 1;
}


