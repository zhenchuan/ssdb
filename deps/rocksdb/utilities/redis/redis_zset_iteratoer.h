/* 
 * File:   redis_zset_iteratoer.h
 * Author: crnsnl
 *
 * Created on April 22, 2014, 4:40 PM
 */

#ifndef REDIS_ZSET_ITERATOER_H
#define	REDIS_ZSET_ITERATOER_H

#include "rocksdb/slice.h"
#include "util/coding.h"
#include <string.h>

namespace rocksdb{
    
    class RedisZsetIterator{
    public:
        /// data的格式 : key(4byte),value(4byte) ...
        RedisZsetIterator(const std::string& data){
           
        }
        
        int Zadd(const Slice& data){
            
        }
        
        int Zincr(const Slice& delta){
            
        }
        
    private:
        const char* const data_;
        std::vector<char> result_;
        
    };
    
}

#endif	/* REDIS_ZSET_ITERATOER_H */

