#include "t_kv.h"
#include "util/strings.h"
#include "rocksdb/write_batch.h"

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

int SSDB::incr_zset(const Bytes &key,const Bytes &by,std::string *new_value,char log_type){

	Transaction trans(binlogs);
	std::string old;
	//rocksdb::Slice value_; //将要重新被写入的value
	std::string value_;
	int ret = this->get(key,&old);
	int total_size = 0;

	if(ret == -1 ){
		return -1;
	}else if(ret == 0){
		value_ = by.String();
		total_size = by.size()/8;
	}else{
		//注意,incr或者set的数据,客户端都是排序好的 || 备注:目前的排序方式已经不再需要.
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
					//log_error("merged:%lld i:%ld i+:%ld j:%d j+:%d vv : %du kk: %du orign_kkvv : %llu"
					//		" v : %lu k: %lu orign_kv : %llu",
					//		olds.at(j),i,size_i,j,size_j,vv,kk,encode_j,v,k,encode_i);
					break;
				}
			}
			if(found==false){
				//log_error("!found %lld %d %d",encode_i,v,k);
				olds.push_back(encode_i);
				//olds.push_back(((uint64_t)(v)) << 32 | k);
			}

		}
		//TODO 更高效的排序方式.
		std::sort(olds.begin(),olds.end());


		std::string buf;
		for(int j=0;j<olds.size();j++){

			char encoded[sizeof(uint64_t)];
			encode_uint64(olds.at(j),encoded);
			value_.append(encoded,sizeof(encoded));

			//log_error("encode %lld %ld",olds.at(j),sizeof(encoded));
			//dump(encoded,8);
		}

		total_size = olds.size();
		//value_ = rocksdb::Slice(buf);然后这个for循环结束后,用value_把
	}

	*new_value = int64_to_str(total_size);
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf,value_);
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);

	rocksdb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("incr error: %s", s.ToString().c_str());
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
	std::string buf = encode_kv_key(key);
	rocksdb::Status s = db->Get(rocksdb::ReadOptions(), buf, val);
	if(s.IsNotFound()){
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
