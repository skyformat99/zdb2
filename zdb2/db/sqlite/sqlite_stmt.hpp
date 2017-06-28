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

#include <sqlite3.h>

#include <zdb2/db/stmt.hpp>
#include <zdb2/db/sqlite/sqlite_util.hpp>

namespace zdb2
{

	class sqlite_stmt : public stmt
	{
	public:
		sqlite_stmt(
			sqlite3 * db,
			const char * sql,
			std::size_t timeout
		)
			: stmt(sql,timeout)
			, m_db(db)
		{
			if (!m_db)
				throw std::runtime_error("invalid parameters.");
			
			_init();
		}

		virtual ~sqlite_stmt()
		{
			close();
		}

		virtual void close() override
		{
			if (m_stmt)
			{
				sqlite3_finalize(m_stmt);
				m_stmt = nullptr;
			}
		}

		/** @name Parameters */
		//@{

		/**
		 * Sets the <i>in</i> parameter at index <code>parameterIndex</code> to the 
		 * given string value. 
		 * @param P A PreparedStatement object
		 * @param parameterIndex The first parameter is 1, the second is 2,..
		 * @param x The string value to set. Must be a NUL terminated string. NULL
		 * is allowed to indicate a SQL NULL value. 
		 * @exception SQLException If a database access error occurs or if parameter 
		 * index is out of range
		 * @see SQLException.h
		*/
		virtual void set_string(int param_index, const char * x) override
		{
			if (m_stmt)
			{
				sqlite3_reset(m_stmt);
				int size = x ? (int)std::strlen(x) : 0;
				if (SQLITE_RANGE == sqlite3_bind_text(m_stmt, param_index, x, size, SQLITE_STATIC))
					throw std::runtime_error("parameter index is out of range.");
			}
		}


		/**
		 * Sets the <i>in</i> parameter at index <code>parameterIndex</code> to the
		 * given int value. 
		 * In general, on both 32 and 64 bits architecture, <code>int</code> is 4 bytes
		 * or 32 bits and <code>long long</code> is 8 bytes or 64 bits. A
		 * <code>long</code> type is usually equal to <code>int</code> on 32 bits
		 * architecture and equal to <code>long long</code> on 64 bits architecture.
		 * However, the width of integer types are architecture and compiler dependent.
		 * The above is usually true, but not necessarily.
		 * @param P A PreparedStatement object
		 * @param parameterIndex The first parameter is 1, the second is 2,..
		 * @param x The int value to set
		 * @exception SQLException If a database access error occurs or if parameter
		 * index is out of range
		 * @see SQLException.h
		 */
		virtual void set_int(int param_index, int x) override
		{
			if (m_stmt)
			{
				sqlite3_reset(m_stmt);
				if (SQLITE_RANGE == sqlite3_bind_int(m_stmt, param_index, x))
					throw std::runtime_error("parameter index is out of range.");
			}
		}


		/**
		 * Sets the <i>in</i> parameter at index <code>parameterIndex</code> to the 
		 * given long long value. 
		 * In general, on both 32 and 64 bits architecture, <code>int</code> is 4 bytes
		 * or 32 bits and <code>long long</code> is 8 bytes or 64 bits. A
		 * <code>long</code> type is usually equal to <code>int</code> on 32 bits
		 * architecture and equal to <code>long long</code> on 64 bits architecture.
		 * However, the width of integer types are architecture and compiler dependent.
		 * The above is usually true, but not necessarily.
		 * @param P A PreparedStatement object
		 * @param parameterIndex The first parameter is 1, the second is 2,..
		 * @param x The long long value to set
		 * @exception SQLException If a database access error occurs or if parameter 
		 * index is out of range
		 * @see SQLException.h
		 */
		virtual void set_int64(int param_index, int64_t x) override
		{
			if (m_stmt)
			{
				sqlite3_reset(m_stmt);
				if (SQLITE_RANGE == sqlite3_bind_int64(m_stmt, param_index, x))
					throw std::runtime_error("parameter index is out of range.");
			}
		}


		/**
		 * Sets the <i>in</i> parameter at index <code>parameterIndex</code> to the 
		 * given double value. 
		 * @param P A PreparedStatement object
		 * @param parameterIndex The first parameter is 1, the second is 2,..
		 * @param x The double value to set
		 * @exception SQLException If a database access error occurs or if parameter 
		 * index is out of range
		 * @see SQLException.h
		 */
		virtual void set_double(int param_index, double x) override
		{
			if (m_stmt)
			{
				sqlite3_reset(m_stmt);
				if (SQLITE_RANGE == sqlite3_bind_double(m_stmt, param_index, x))
					throw std::runtime_error("parameter index is out of range.");
			}
		}


		/**
		 * Sets the <i>in</i> parameter at index <code>parameterIndex</code> to the 
		 * given blob value. 
		 * @param P A PreparedStatement object
		 * @param parameterIndex The first parameter is 1, the second is 2,..
		 * @param x The blob value to set
		 * @param size The number of bytes in the blob 
		 * @exception SQLException If a database access error occurs or if parameter 
		 * index is out of range
		 * @see SQLException.h
		 */
		virtual void set_blob(int param_index, const void * x, std::size_t size) override
		{
			if (m_stmt)
			{
				sqlite3_reset(m_stmt);
				if (SQLITE_RANGE == sqlite3_bind_blob(m_stmt, param_index, x, (int)size, SQLITE_STATIC))
					throw std::runtime_error("parameter index is out of range.");
			}
		}


		/**
		 * Sets the <i>in</i> parameter at index <code>parameterIndex</code> to the
		 * given Unix timestamp value. The timestamp value given in <code>x</code>
		 * is expected to be in the GMT timezone. For instance, a value returned by
		 * time(3) which represents the system's notion of the current Greenwich time.
		 * <i class="textinfo">SQLite</i> does not have temporal SQL data types per se
		 * and using this method with SQLite will store the timestamp value as a numerical
		 * type as-is. This is usually what you want anyway, since it is fast, compact and
		 * unambiguous.
		 * @param P A PreparedStatement object
		 * @param parameterIndex The first parameter is 1, the second is 2,..
		 * @param x The GMT timestamp value to set. E.g. a value returned by time(3)
		 * @exception SQLException If a database access error occurs or if parameter
		 * index is out of range
		 * @see SQLException.h ResultSet_getTimestamp
		 */
		virtual void set_timestamp(int param_index, time_t x) override
		{
			if (m_stmt)
			{
				sqlite3_reset(m_stmt);
				if (SQLITE_RANGE == sqlite3_bind_int64(m_stmt, param_index, x))
					throw std::runtime_error("parameter index is out of range.");
			}
		}

		//@}

		/**
		 * Executes the prepared SQL statement, which may be an INSERT, UPDATE,
		 * or DELETE statement or an SQL statement that returns nothing, such
		 * as an SQL DDL statement. 
		 * @param P A PreparedStatement object
		 * @exception SQLException If a database error occurs
		 * @see SQLException.h
		 */
		virtual void execute() override
		{
			int status = 0;
#if defined SQLITEUNLOCK && SQLITE_VERSION_NUMBER >= 3006012
			status = sqlite_util::sqlite3_blocking_step(m_stmt);
#else
			status = sqlite_util::execute(m_timeout, sqlite3_step, m_stmt);
#endif
			switch (status)
			{
			case SQLITE_DONE:
				status = sqlite3_reset(m_stmt);
				break;
			case SQLITE_ROW:
				status = sqlite3_reset(m_stmt);
				throw std::runtime_error("select statement not allowed in execute().");
				break;
			default:
				status = sqlite3_reset(m_stmt);
				throw std::runtime_error(sqlite3_errmsg(m_db));
				break;
			}
		}


		/**
		 * Returns the number of rows that was inserted, deleted or modified by the
		 * most recently completed SQL statement on the database connection. If used
		 * with a transaction, this method should be called <i>before</i> commit is
		 * executed, otherwise 0 is returned.
		 * @param P A PreparedStatement object
		 * @return The number of rows changed by the last (DIM) SQL statement
		 */
		virtual int64_t rows_changed() override
		{
			return (m_db ? (int64_t)sqlite3_changes(m_db) : 0);
		}


		/** @name Properties */
		//@{


		//@}


	protected:
		virtual void _init() override
		{
			if (m_db && !m_sql.empty())
			{
				int status;
				const char * tail;

#if defined SQLITEUNLOCK && SQLITE_VERSION_NUMBER >= 3006012
				status = sqlite_util::sqlite3_blocking_prepare_v2(m_db, m_sql.c_str(), -1, &m_stmt, &tail);
#elif SQLITE_VERSION_NUMBER >= 3004000
				status = sqlite_util::execute(m_timeout, sqlite3_prepare_v2, m_db, m_sql.c_str(), -1, &m_stmt, &tail);
#else
				status = sqlite_util::execute(m_timeout, sqlite3_prepare, m_db, m_sql.c_str(), -1, &m_stmt, &tail);
#endif

				if (status == SQLITE_OK)
				{
					m_param_count = sqlite3_bind_parameter_count(m_stmt);
				}
			}
		}

	protected:
		sqlite3 * m_db = nullptr;

		sqlite3_stmt * m_stmt = nullptr;

	};

}
