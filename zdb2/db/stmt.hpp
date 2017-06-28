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
#include <stdexcept>

#include <zdb2/config.hpp>

namespace zdb2
{

	class stmt
	{
	public:
		stmt(const char * sql, std::size_t timeout) : m_timeout(timeout)
		{
			if (!sql || sql[0] == '\0')
				throw std::runtime_error("error : invalid parameters.");
			else
				m_sql = sql;
		}

		virtual ~stmt()
		{

		}

		virtual void close() = 0;

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
		virtual void set_string(int param_index, const char * x) = 0;


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
		virtual void set_int(int param_index, int x) = 0;


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
		virtual void set_int64(int param_index, int64_t x) = 0;


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
		virtual void set_double(int param_index, double x) = 0;


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
		virtual void set_blob(int param_index, const void * x, std::size_t size) = 0;


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
		virtual void set_timestamp(int param_index, time_t x) = 0;

		//@}

		/**
		 * Executes the prepared SQL statement, which may be an INSERT, UPDATE,
		 * or DELETE statement or an SQL statement that returns nothing, such
		 * as an SQL DDL statement. 
		 * @param P A PreparedStatement object
		 * @exception SQLException If a database error occurs
		 * @see SQLException.h
		 */
		virtual void execute() = 0;


		/**
		 * Returns the number of rows that was inserted, deleted or modified by the
		 * most recently completed SQL statement on the database connection. If used
		 * with a transaction, this method should be called <i>before</i> commit is
		 * executed, otherwise 0 is returned.
		 * @param P A PreparedStatement object
		 * @return The number of rows changed by the last (DIM) SQL statement
		 */
		virtual int64_t rows_changed() = 0;


		/** @name Properties */
		//@{

		/**
		 * Returns the number of parameters in this prepared statement.
		 * @param P A PreparedStatement object
		 * @return The number of parameters in this prepared statement
		 */
		virtual int get_param_count()
		{
			return m_param_count;
		}

		//@}

	protected:
		virtual void _init() = 0;

	protected:

		std::size_t m_timeout = zdb2::DEFAULT_TIMEOUT;

		int m_param_count = 0;

		std::string m_sql;

	};

}
