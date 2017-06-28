/*
 * COPYRIGHT (C) 2017, zhllxt
 *
 * Author   : zhllxt
 * QQ       : 37792738
 * Email    : 37792738@qq.com
 * 
 */


#pragma once

#include <cctype>
#include <string>
#include <memory>
#include <algorithm>
#include <mutex>
#include <thread>
#include <condition_variable>

#include <libpq-fe.h>

namespace zdb2
{

	class postgresql_util
	{
	public:

		const static my_bool yes = true;

		const static my_bool no = false;

		const static int MYSQL_OK = 0;

		const static int STRLEN = 256;

		typedef struct param_t {
			union {
				int integer;
				long long llong;
				double real;
				MYSQL_TIME timestamp;
			} type;
			unsigned long length;
		} param_t;

		typedef struct column_t {
			my_bool is_null;
			MYSQL_FIELD * field;
			unsigned long length;
			char * buffer;
		} column_t;

	};


}
