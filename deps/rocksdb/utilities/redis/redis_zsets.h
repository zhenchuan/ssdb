#ifndef ROCKSDB_LITE
#pragma once

#include <string>
#include "rocksdb/db.h"

namespace rocksdb {

	class RedisZsets{
	public:
		RedisZsets(const std::string& db_path,
		             Options options, bool destructive = false);

	public:
		int Zadd(const std::string& key,const std::string& member,int32_t score);
		int ZincrBy(const std::string& key,const std::string& member,int32_t increment);
		std::map<std::string,int32_t> Zrange(const std::string& key,
		                                 int32_t first, int32_t last);
	private:
		 std::string db_name_;       // The actual database name/path
		 WriteOptions put_option_;
		 ReadOptions get_option_;
		 std::unique_ptr<DB> db_;
	};


} // namespace rocksdb
#endif  // ROCKSDB_LITE
