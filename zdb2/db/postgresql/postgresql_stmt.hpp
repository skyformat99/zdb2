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

#include <libpq-fe.h>

#include <zdb2/db/stmt.hpp>
#include <zdb2/db/postgresql/postgresql_util.hpp>

namespace zdb2
{

	class postgresql_stmt : public stmt
	{
	public:
		postgresql_stmt(
			MYSQL * db,
			const char * sql,
			std::size_t timeout
		)
			: stmt(sql, timeout)
			, m_db(db)
		{
			if (!m_db)
				throw std::runtime_error("invalid parameters.");

			_init();
		}

		virtual ~postgresql_stmt()
		{
			close();
		}

		virtual void close() override
		{
			if (m_stmt)
			{
				mysql_stmt_close(m_stmt);
				m_stmt = nullptr;
			}
			if (m_bind)
			{
				delete[]m_bind;
				m_bind = nullptr;
			}
			if (m_params)
			{
				delete[]m_params;
				m_params = nullptr;
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
			if (m_stmt && m_bind && m_params && param_index >= 0)
			{
				m_bind[param_index].buffer_type = MYSQL_TYPE_STRING;
				m_bind[param_index].buffer = (char*)x;

				if (!x)
				{
					m_params[param_index].length = 0;
					m_bind[param_index].is_null = const_cast<my_bool *>(&mysql_util::yes);
				}
				else
				{
					m_params[param_index].length = (unsigned long)std::strlen(x);
					m_bind[param_index].is_null = const_cast<my_bool *>(&mysql_util::no);
				}

				m_bind[param_index].length = &m_params[param_index].length;
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
			if (m_stmt && m_bind && m_params && param_index >= 0)
			{
				m_params[param_index].type.integer = x;
				m_bind[param_index].buffer_type = MYSQL_TYPE_LONG;
				m_bind[param_index].buffer = &m_params[param_index].type.integer;
				m_bind[param_index].is_null = const_cast<my_bool *>(&mysql_util::no);
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
			if (m_stmt && m_bind && m_params && param_index >= 0)
			{
				m_params[param_index].type.llong = x;
				m_bind[param_index].buffer_type = MYSQL_TYPE_LONGLONG;
				m_bind[param_index].buffer = &m_params[param_index].type.llong;
				m_bind[param_index].is_null = const_cast<my_bool *>(&mysql_util::no);
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
			if (m_stmt && m_bind && m_params && param_index >= 0)
			{
				m_params[param_index].type.real = x;
				m_bind[param_index].buffer_type = MYSQL_TYPE_DOUBLE;
				m_bind[param_index].buffer = &m_params[param_index].type.real;
				m_bind[param_index].is_null = const_cast<my_bool *>(&mysql_util::no);
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
			if (m_stmt && m_bind && m_params && param_index >= 0)
			{
				m_bind[param_index].buffer_type = MYSQL_TYPE_BLOB;
				m_bind[param_index].buffer = (void*)x;

				if (!x)
				{
					m_params[param_index].length = 0;
					m_bind[param_index].is_null = const_cast<my_bool *>(&mysql_util::yes);
				}
				else
				{
					m_params[param_index].length = (unsigned long)size;
					m_bind[param_index].is_null = const_cast<my_bool *>(&mysql_util::no);
				}

				m_bind[param_index].length = &m_params[param_index].length;
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
			if (m_stmt && m_bind && m_params && param_index >= 0)
			{
				struct tm * ptm = std::gmtime(const_cast<const time_t *>(&x));

				m_params[param_index].type.timestamp.year = ptm->tm_year + 1900;
				m_params[param_index].type.timestamp.month = ptm->tm_mon + 1;
				m_params[param_index].type.timestamp.day = ptm->tm_mday;
				m_params[param_index].type.timestamp.hour = ptm->tm_hour;
				m_params[param_index].type.timestamp.minute = ptm->tm_min;
				m_params[param_index].type.timestamp.second = ptm->tm_sec;

				m_bind[param_index].buffer_type = MYSQL_TYPE_TIMESTAMP;
				m_bind[param_index].buffer = &m_params[param_index].type.timestamp;

				m_bind[param_index].is_null = const_cast<my_bool *>(&mysql_util::no);
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
			if (m_param_count > 0 && m_stmt && m_bind && m_params)
			{
				if (mysql_util::MYSQL_OK != mysql_stmt_bind_param(m_stmt, m_bind))
					throw std::runtime_error(mysql_stmt_error(m_stmt));

#if MYSQL_VERSION_ID >= 50002
				unsigned long cursor = CURSOR_TYPE_NO_CURSOR;
				mysql_stmt_attr_set(m_stmt, STMT_ATTR_CURSOR_TYPE, &cursor);
#endif

				if ((mysql_util::MYSQL_OK != mysql_stmt_execute(m_stmt)))
					throw std::runtime_error(mysql_stmt_error(m_stmt));

				/* Discard prepared param data in client/server */
				mysql_stmt_reset(m_stmt);
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
			return (m_stmt ? (int64_t)mysql_stmt_affected_rows(m_stmt) : 0);
		}


		/** @name Properties */
		//@{


		//@}


	protected:
		virtual void _init() override
		{

		}

	protected:
		MYSQL * m_db = nullptr;

		MYSQL_STMT * m_stmt = nullptr;

		MYSQL_BIND * m_bind = nullptr;

		mysql_util::param_t * m_params = nullptr;
	};

}
