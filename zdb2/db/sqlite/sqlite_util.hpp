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

#include <sqlite3.h>

namespace zdb2
{

	class sqlite_util
	{
	public:

#if defined SQLITEUNLOCK && SQLITE_VERSION_NUMBER >= 3006012

		/* SQLite unlock notify based synchronization */

		typedef struct UnlockNotification {
			bool fired;
			std::condition_variable cv;
			std::mutex mtx;
		} UnlockNotification_T;

		static inline void unlock_notify_cb(void **apArg, int nArg)
		{
			for (int i = 0; i < nArg; i++)
			{
				UnlockNotification_T *p = (UnlockNotification_T *)apArg[i];
				std::unique_lock<std::mutex> lck(p->mtx);
				p->fired = true;
				p->cv.notify_all();
			}
		}

		static inline int wait_for_unlock_notify(sqlite3 *db)
		{
			UnlockNotification_T un;
			un.fired = false;

			int rc = sqlite3_unlock_notify(db, unlock_notify_cb, (void *)&un);
			assert(rc == SQLITE_LOCKED || rc == SQLITE_OK);

			if (rc == SQLITE_OK)
			{
				std::unique_lock <std::mutex> lck(un.mtx);
				while (!un.fired)
					un.cv.wait(lck);
			}

			return rc;
		}

		static inline int sqlite3_blocking_step(sqlite3_stmt *stmt)
		{
			int rc;
			while (SQLITE_LOCKED == (rc = sqlite3_step(stmt)))
			{
				rc = wait_for_unlock_notify(sqlite3_db_handle(stmt));
				if (rc != SQLITE_OK)
					break;
				sqlite3_reset(stmt);
			}
			return rc;
		}

		static inline int sqlite3_blocking_prepare_v2(sqlite3 *db, const char *zSql, int nSql, sqlite3_stmt **stmt, const char **pz)
		{
			int rc;
			while (SQLITE_LOCKED == (rc = sqlite3_prepare_v2(db, zSql, nSql, stmt, pz)))
			{
				rc = wait_for_unlock_notify(db);
				if (rc != SQLITE_OK)
					break;
			}
			return rc;
		}

		static inline int sqlite3_blocking_exec(sqlite3 *db, const char *zSql, int(*callback)(void *, int, char **, char **), void *arg, char **errmsg)
		{
			int rc;
			while (SQLITE_LOCKED == (rc = sqlite3_exec(db, zSql, callback, arg, errmsg)))
			{
				rc = wait_for_unlock_notify(db);
				if (rc != SQLITE_OK)
					break;
			}
			return rc;
		}

#else
		/* SQLite timed retry macro */

		template<typename _handler, typename... Args>
		static inline int execute(std::size_t timeout, _handler handler, Args... handler_args)
		{
			int status = 0;
			int x = 0;
			while (true)
			{
				status = handler(handler_args...);

				if (((status == SQLITE_BUSY) || (status == SQLITE_LOCKED)) && (x++ <= 9))
					std::this_thread::sleep_for(std::chrono::milliseconds(timeout / 100));
				else
					break;
			}
			return status;
		}

#endif


	};

}
