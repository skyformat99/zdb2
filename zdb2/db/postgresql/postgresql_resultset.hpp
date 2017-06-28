/*
 * COPYRIGHT (C) 2017, zhllxt
 *
 * Author   : zhllxt
 * QQ       : 37792738
 * Email    : 37792738@qq.com
 * 
 */


#pragma once

#include <cassert>
#include <cctype>
#include <string>
#include <memory>
#include <algorithm>
#include <mutex>
#include <unordered_map>
#include <ctime>

#include <libpq-fe.h>

#include <zdb2/db/resultset.hpp>
#include <zdb2/db/postgresql/postgresql_util.hpp>

namespace zdb2
{

#pragma warning(disable:4996)

	class postgresql_resultset : public resultset
	{
	public:
		postgresql_resultset(
			MYSQL_STMT * stmt,
			std::size_t timeout = zdb2::DEFAULT_TIMEOUT
		)
			: resultset(timeout)
			, m_stmt(stmt)
		{
			assert(m_stmt);
			if (!m_stmt)
				throw std::runtime_error("error : invalid parameters.");

			_init();
		}

		virtual ~postgresql_resultset()
		{
			close();
		}

		virtual void close() override
		{
			if (m_stmt)
			{
				mysql_stmt_free_result(m_stmt);
				mysql_stmt_close(m_stmt);
				m_stmt = nullptr;
			}
			if (m_meta)
			{
				mysql_free_result(m_meta);
				m_meta = nullptr;
			}
			if (m_bind)
			{
				delete[]m_bind;
				m_bind = nullptr;
			}
			if (m_columns)
			{
				for (int i = 0; i < m_column_count; i++)
				{
					std::free((void *)m_columns[i].buffer);
				}
				delete[]m_columns;
				m_columns = nullptr;
			}
		}
		
		/**
		 * Returns the number of columns in this ResultSet object.
		 * @param R A ResultSet object
		 * @return The number of columns
		 */
		virtual int get_column_count() override
		{
			return (m_stmt ? (int)mysql_stmt_field_count(m_stmt) : 0);
		}


		/**
		 * Get the designated column's name.
		 * @param R A ResultSet object
		 * @param columnIndex The first column is 1, the second is 2, ...
		 * @return Column name or NULL if the column does not exist. You 
		 * should use the method ResultSet_getColumnCount() to test for 
		 * the availability of columns in the result set.
		 */
		virtual const char * get_column_name(int column_index) override
		{
			if (m_column_count <= 0 || column_index < 0 || column_index >= m_column_count)
				return nullptr;
			return m_columns[column_index].field->name;
		}

		/**
		 * @function : get column index by column name
		 */
		virtual int get_column_index(const char * column_name) override
		{
			auto iterator = m_column_name_map.find(column_name);
			if (iterator != m_column_name_map.end())
				return iterator->second;
			return -1;
		}

		/**
		 * Returns column size in bytes. If the column is a blob then 
		 * this method returns the number of bytes in that blob. No type 
		 * conversions occur. If the result is a string (or a number 
		 * since a number can be converted into a string) then return the 
		 * number of bytes in the resulting string. 
		 * @param R A ResultSet object
		 * @param columnIndex The first column is 1, the second is 2, ...
		 * @return Column data size
		 * @exception SQLException If columnIndex is outside the valid range
		 * @see SQLException.h
		 */
		virtual std::size_t get_column_size(int column_index) override
		{
			if (m_column_count <= 0 || column_index < 0 || column_index >= m_column_count)
				return 0;
			if (m_columns[column_index].is_null)
				return 0;
			return m_columns[column_index].length;
		}

		//@}

		/**
		 * Moves the cursor down one row from its current position. A
		 * ResultSet cursor is initially positioned before the first row; the
		 * first call to this method makes the first row the current row; the
		 * second call makes the second row the current row, and so on. When
		 * there are not more available rows false is returned. An empty
		 * ResultSet will return false on the first call to ResultSet_next().
		 * @param R A ResultSet object
		 * @return true if the new current row is valid; false if there are no
		 * more rows
		 * @exception SQLException If a database access error occurs
		 */
		virtual bool next_row() override
		{
			if (!m_stmt || !m_meta || m_column_count <= 0)
				return false;

			if (m_need_rebind)
			{
				if ((mysql_util::MYSQL_OK != mysql_stmt_bind_result(m_stmt, m_bind)))
					throw std::runtime_error(mysql_stmt_error(m_stmt));
				m_need_rebind = false;
			}

			int status = mysql_stmt_fetch(m_stmt);
			if (1 == status)
				throw std::runtime_error(mysql_stmt_error(m_stmt));

			return ((status == mysql_util::MYSQL_OK) || (status == MYSQL_DATA_TRUNCATED));
		}

		/** @name Columns */
		//@{

		/**
		 * Returns true if the value of the designated column in the current row of
		 * this ResultSet object is SQL NULL, otherwise false. If the column value is 
		 * SQL NULL, a Result Set returns the NULL pointer for string and blob values
		 * and 0 for primitive data types. Use this method if you need to differ 
		 * between SQL NULL and the value NULL/0.
		 * @param R A ResultSet object
		 * @param columnIndex The first column is 1, the second is 2, ...
		 * @return True if column value is SQL NULL, otherwise false
		 * @exception SQLException If a database access error occurs or
		 * columnIndex is outside the valid range
		 * @see SQLException.h
		 */
		virtual bool is_null(int column_index) override
		{
			if (m_column_count <= 0 || column_index < 0 || column_index >= m_column_count)
				return true;

			return (m_columns[column_index].is_null);
		}



		/**
		 * Retrieves the value of the designated column in the current row of
		 * this ResultSet object as a C-string. If <code>columnIndex</code>
		 * is outside the range [1..ResultSet_getColumnCount()] this
		 * method throws an SQLException. <i>The returned string may only be 
		 * valid until the next call to ResultSet_next() and if you plan to use
		 * the returned value longer, you must make a copy.</i>
		 * @param R A ResultSet object
		 * @param columnIndex The first column is 1, the second is 2, ...
		 * @return The column value; if the value is SQL NULL, the value
		 * returned is NULL
		 * @exception SQLException If a database access error occurs or
		 * columnIndex is outside the valid range
		 * @see SQLException.h
		 */
		virtual const char * get_string(int column_index) override
		{
			if (m_stmt && m_bind && m_columns && column_index >= 0 && column_index < m_column_count)
			{
				if (m_columns[column_index].is_null)
					return nullptr;

				_ensure_capacity(column_index);

				m_columns[column_index].buffer[m_columns[column_index].length] = 0;
				return m_columns[column_index].buffer;
			}
			return nullptr;
		}


		/**
		 * Retrieves the value of the designated column in the current row of
		 * this ResultSet object as a C-string. If <code>columnName</code>
		 * is not found this method throws an SQLException. <i>The returned string
		 * may only be valid until the next call to ResultSet_next() and if you plan
		 * to use the returned value longer, you must make a copy.</i>
		 * @param R A ResultSet object
		 * @param columnName The SQL name of the column. <i>case-sensitive</i>
		 * @return The column value; if the value is SQL NULL, the value
		 * returned is NULL
		 * @exception SQLException If a database access error occurs or
		 * columnName does not exist
		 * @see SQLException.h
		 */
		virtual const char * get_string(const char * column_name) override
		{
			int col_index = get_column_index(column_name);
			return ((col_index >= 0) ? get_string(col_index) : nullptr);
		}


		/**
		 * Retrieves the value of the designated column in the current row of this 
		 * ResultSet object as an int. If <code>columnIndex</code> is outside the 
		 * range [1..ResultSet_getColumnCount()] this method throws an SQLException. 
		 * In general, on both 32 and 64 bits architecture, <code>int</code> is 4 bytes
		 * or 32 bits and <code>long long</code> is 8 bytes or 64 bits. A
		 * <code>long</code> type is usually equal to <code>int</code> on 32 bits
		 * architecture and equal to <code>long long</code> on 64 bits architecture.
		 * However, the width of integer types are architecture and compiler dependent.
		 * The above is usually true, but not necessarily.
		 * @param R A ResultSet object
		 * @param columnIndex The first column is 1, the second is 2, ...
		 * @return The column value; if the value is SQL NULL, the value
		 * returned is 0
		 * @exception SQLException If a database access error occurs, columnIndex
		 * is outside the valid range or if the value is NaN
		 * @see SQLException.h
		 */
		virtual int get_int(int column_index) override
		{
			auto s = get_string(column_index);
			return (s ? std::atoi(s) : -1);
		}


		/**
		 * Retrieves the value of the designated column in the current row of
		 * this ResultSet object as an int. If <code>columnName</code> is
		 * not found this method throws an SQLException.
		 * In general, on both 32 and 64 bits architecture, <code>int</code> is 4 bytes
		 * or 32 bits and <code>long long</code> is 8 bytes or 64 bits. A
		 * <code>long</code> type is usually equal to <code>int</code> on 32 bits
		 * architecture and equal to <code>long long</code> on 64 bits architecture.
		 * However, the width of integer types are architecture and compiler dependent.
		 * The above is usually true, but not necessarily.
		 * @param R A ResultSet object
		 * @param columnName The SQL name of the column. <i>case-sensitive</i>
		 * @return The column value; if the value is SQL NULL, the value
		 * returned is 0
		 * @exception SQLException If a database access error occurs, columnName
		 * does not exist or if the value is NaN
		 * @see SQLException.h
		 */
		virtual int get_int(const char * column_name) override
		{
			auto s = get_string(column_name);
			return (s ? std::atoi(s) : -1);
		}


		/**
		 * Retrieves the value of the designated column in the current row of
		 * this ResultSet object as a long long. If <code>columnIndex</code>
		 * is outside the range [1..ResultSet_getColumnCount()] this
		 * method throws an SQLException.
		 * In general, on both 32 and 64 bits architecture, <code>int</code> is 4 bytes
		 * or 32 bits and <code>long long</code> is 8 bytes or 64 bits. A 
		 * <code>long</code> type is usually equal to <code>int</code> on 32 bits 
		 * architecture and equal to <code>long long</code> on 64 bits architecture.
		 * However, the width of integer types are architecture and compiler dependent.
		 * The above is usually true, but not necessarily.
		 * @param R A ResultSet object
		 * @param columnIndex The first column is 1, the second is 2, ...
		 * @return The column value; if the value is SQL NULL, the value
		 * returned is 0
		 * @exception SQLException If a database access error occurs,
		 * columnIndex is outside the valid range or if the value is NaN
		 * @see SQLException.h
		 */
		virtual int64_t get_int64(int column_index) override
		{
			auto s = get_string(column_index);
			return (s ? (int64_t)std::atoll(s) : -1);
		}


		/**
		 * Retrieves the value of the designated column in the current row of
		 * this ResultSet object as a long long. If <code>columnName</code>
		 * is not found this method throws an SQLException.
		 * In general, on both 32 and 64 bits architecture, <code>int</code> is 4 bytes
		 * or 32 bits and <code>long long</code> is 8 bytes or 64 bits. A
		 * <code>long</code> type is usually equal to <code>int</code> on 32 bits
		 * architecture and equal to <code>long long</code> on 64 bits architecture.
		 * However, the width of integer types are architecture and compiler dependent.
		 * The above is usually true, but not necessarily.
		 * @param R A ResultSet object
		 * @param columnName The SQL name of the column. <i>case-sensitive</i>
		 * @return The column value; if the value is SQL NULL, the value
		 * returned is 0
		 * @exception SQLException If a database access error occurs, columnName
		 * does not exist or if the value is NaN
		 * @see SQLException.h
		 */
		virtual int64_t get_int64(const char * column_name) override
		{
			auto s = get_string(column_name);
			return (s ? (int64_t)std::atoll(s) : -1);
		}


		/**
		 * Retrieves the value of the designated column in the current row of
		 * this ResultSet object as a double. If <code>columnIndex</code>
		 * is outside the range [1..ResultSet_getColumnCount()] this
		 * method throws an SQLException.
		 * @param R A ResultSet object
		 * @param columnIndex The first column is 1, the second is 2, ...
		 * @return The column value; if the value is SQL NULL, the value
		 * returned is 0.0
		 * @exception SQLException If a database access error occurs, columnIndex
		 * is outside the valid range or if the value is NaN
		 * @see SQLException.h
		 */
		virtual double get_double(int column_index) override
		{
			auto s = get_string(column_index);
			return (s ? std::atof(s) : -1.f);
		}


		/**
		 * Retrieves the value of the designated column in the current row of
		 * this ResultSet object as a double. If <code>columnName</code> is
		 * not found this method throws an SQLException.
		 * @param R A ResultSet object
		 * @param columnName The SQL name of the column. <i>case-sensitive</i>
		 * @return The column value; if the value is SQL NULL, the value
		 * returned is 0.0
		 * @exception SQLException If a database access error occurs, columnName
		 * does not exist or if the value is NaN
		 * @see SQLException.h
		 */
		virtual double get_double(const char * column_name) override
		{
			auto s = get_string(column_name);
			return (s ? std::atof(s) : -1.f);
		}


		/**
		 * Retrieves the value of the designated column in the current row of
		 * this ResultSet object as a void pointer. If <code>columnIndex</code>
		 * is outside the range [1..ResultSet_getColumnCount()] this method 
		 * throws an SQLException. <i>The returned blob may only be valid until
		 * the next call to ResultSet_next() and if you plan to use the returned
		 * value longer, you must make a copy.</i> 
		 * @param R A ResultSet object
		 * @param columnIndex The first column is 1, the second is 2, ...
		 * @param size The number of bytes in the blob is stored in size 
		 * @return The column value; if the value is SQL NULL, the value
		 * returned is NULL
		 * @exception SQLException If a database access error occurs or 
		 * columnIndex is outside the valid range
		 * @see SQLException.h
		 */
		virtual const void * get_blob(int column_index, std::size_t * size) override
		{
			if (m_stmt && m_bind && m_columns && column_index >= 0 && column_index < m_column_count)
			{
				if (m_columns[column_index].is_null)
					return nullptr;

				_ensure_capacity(column_index);

				*size = m_columns[column_index].length;

				return (const void *)m_columns[column_index].buffer;
			}
			return nullptr;
		}


		/**
		 * Retrieves the value of the designated column in the current row of
		 * this ResultSet object as a void pointer. If <code>columnName</code>
		 * is not found this method throws an SQLException. <i>The returned
		 * blob may only be valid until the next call to ResultSet_next() and if 
		 * you plan to use the returned value longer, you must make a copy.</i>
		 * @param R A ResultSet object
		 * @param columnName The SQL name of the column. <i>case-sensitive</i>
		 * @param size The number of bytes in the blob is stored in size 
		 * @return The column value; if the value is SQL NULL, the value
		 * returned is NULL
		 * @exception SQLException If a database access error occurs or 
		 * columnName does not exist
		 * @see SQLException.h
		 */
		virtual const void * get_blob(const char * column_name, std::size_t * size) override
		{
			int col_index = get_column_index(column_name);
			return ((col_index >= 0) ? get_blob(col_index, size) : nullptr);
		}

		//@}

		/** @name Date and Time  */
		//@{

		/**
		 * Retrieves the value of the designated column in the current row of this
		 * ResultSet object as a Unix timestamp. The returned value is in Coordinated
		 * Universal Time (UTC) and represent seconds since the <strong>epoch</strong>
		 * (January 1, 1970, 00:00:00 GMT).
		 *
		 * Even though the underlying database might support timestamp ranges before
		 * the epoch and after '2038-01-19 03:14:07 UTC' it is safest not to assume or
		 * use values outside this range. Especially on a 32-bits system.
		 *
		 * <i class="textinfo">SQLite</i> does not have temporal SQL data types per se
		 * and using this method with SQLite assume the column value in the Result Set
		 * to be either a numerical value representing a Unix Time in UTC which is
		 * returned as-is or an <a href="http://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a>
		 * time string which is converted to a time_t value.
		 * See also PreparedStatement_setTimestamp()
		 * @param R A ResultSet object
		 * @param columnIndex The first column is 1, the second is 2, ...
		 * @return The column value as seconds since the epoch in the 
		 * <i class="textinfo">GMT timezone</i>. If the value is SQL NULL, the
		 * value returned is 0, i.e. January 1, 1970, 00:00:00 GMT
		 * @exception SQLException If a database access error occurs, if 
		 * <code>columnIndex</code> is outside the range [1..ResultSet_getColumnCount()]
		 * or if the column value cannot be converted to a valid timestamp
		 * @see SQLException.h PreparedStatement_setTimestamp
		 */
		virtual time_t get_timestamp(int column_index) override
		{
			// Not integer storage class, try to parse as time string
			return (time_t)0;
		}


		/**
		 * Retrieves the value of the designated column in the current row of this
		 * ResultSet object as a Unix timestamp. The returned value is in Coordinated
		 * Universal Time (UTC) and represent seconds since the <strong>epoch</strong>
		 * (January 1, 1970, 00:00:00 GMT).
		 *
		 * Even though the underlying database might support timestamp ranges before
		 * the epoch and after '2038-01-19 03:14:07 UTC' it is safest not to assume or
		 * use values outside this range. Especially on a 32-bits system.
		 *
		 * <i class="textinfo">SQLite</i> does not have temporal SQL data types per se
		 * and using this method with SQLite assume the column value in the Result Set
		 * to be either a numerical value representing a Unix Time in UTC which is 
		 * returned as-is or an <a href="http://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a>
		 * time string which is converted to a time_t value.
		 * See also PreparedStatement_setTimestamp()
		 * @param R A ResultSet object
		 * @param columnName The SQL name of the column. <i>case-sensitive</i>
		 * @return The column value as seconds since the epoch in the
		 * <i class="textinfo">GMT timezone</i>. If the value is SQL NULL, the
		 * value returned is 0, i.e. January 1, 1970, 00:00:00 GMT
		 * @exception SQLException If a database access error occurs, if 
		 * <code>columnName</code> is not found or if the column value cannot be 
		 * converted to a valid timestamp
		 * @see SQLException.h PreparedStatement_setTimestamp
		 */
		virtual time_t get_timestamp(const char * column_name) override
		{
			int col_index = get_column_index(column_name);
			return ((col_index >= 0) ? get_timestamp(col_index) : (time_t)0);
		}


		/**
		 * Retrieves the value of the designated column in the current row of this
		 * ResultSet object as a Date, Time or DateTime. This method can be used to
		 * retrieve the value of columns with the SQL data type, Date, Time, DateTime
		 * or Timestamp. The returned <code>tm</code> structure follows the convention
		 * for usage with mktime(3) where, tm_hour = hours since midnight [0-23],
		 * tm_min = minutes after the hour [0-59], tm_sec = seconds after the minute
		 * [0-60], tm_mday = day of the month [1-31] and tm_mon = months since January
		 * <b class="textnote">[0-11]</b>. If the column value contains timezone 
		 * information, tm_gmtoff is set to the offset from UTC in seconds, otherwise
		 * tm_gmtoff is set to 0. <i>On systems without tm_gmtoff, (Solaris), the 
		 * member, tm_wday is set to gmt offset instead as this property is ignored 
		 * by mktime on input.</i> The exception to the above is <b class="textnote">tm_year</b> 
		 * which contains the year literal and <i>not years since 1900</i> which is the
		 * convention. All other fields in the structure are set to zero. If the 
		 * column type is DateTime or Timestamp all the fields mentioned above are 
		 * set, if it is a Date or Time, only the relevant fields are set.
		 *
		 * @param R A ResultSet object
		 * @param columnIndex The first column is 1, the second is 2, ...
		 * @return A tm structure with fields for date and time. If the value
		 * is SQL NULL, a zeroed tm structure is returned
		 * @exception SQLException If a database access error occurs, if
		 * <code>columnIndex</code> is outside the range [1..ResultSet_getColumnCount()] 
		 * or if the column value cannot be converted to a valid SQL Date, Time or 
		 * DateTime type
		 * @see SQLException.h
		 */
		virtual tm get_datetime(int column_index) override
		{
			struct tm tm = { 0 };
			if (!m_stmt)
				return tm;
			//if (sqlite3_column_type(m_stmt, column_index) == SQLITE_INTEGER)
			//{
			//	time_t utc = (time_t)sqlite3_column_int64(m_stmt, column_index);
			//	struct tm * utc_tm = std::gmtime(&utc);
			//	if (utc_tm)
			//		utc_tm->tm_year += 1900; // Use year literal
			//	return (*utc_tm);
			//}
			//else
			//{
			//	// Not integer storage class, try to parse as time string

			//}
			return tm;
		}


		/**
		 * Retrieves the value of the designated column in the current row of this
		 * ResultSet object as a Date, Time or DateTime. This method can be used to
		 * retrieve the value of columns with the SQL data type, Date, Time, DateTime
		 * or Timestamp. The returned <code>tm</code> structure follows the convention
		 * for usage with mktime(3) where, tm_hour = hours since midnight [0-23],
		 * tm_min = minutes after the hour [0-59], tm_sec = seconds after the minute
		 * [0-60], tm_mday = day of the month [1-31] and tm_mon = months since January
		 * <b class="textnote">[0-11]</b>. If the column value contains timezone
		 * information, tm_gmtoff is set to the offset from UTC in seconds, otherwise
		 * tm_gmtoff is set to 0. <i>On systems without tm_gmtoff, (Solaris), the
		 * member, tm_wday is set to gmt offset instead as this property is ignored
		 * by mktime on input.</i> The exception to the above is <b class="textnote">tm_year</b>
		 * which contains the year literal and <i>not years since 1900</i> which is the
		 * convention. All other fields in the structure are set to zero. If the
		 * column type is DateTime or Timestamp all the fields mentioned above are
		 * set, if it is a Date or Time, only the relevant fields are set.
		 *
		 * @param R A ResultSet object
		 * @param columnName The SQL name of the column. <i>case-sensitive</i>
		 * @return A tm structure with fields for date and time. If the value
		 * is SQL NULL, a zeroed tm structure is returned
		 * @exception SQLException If a database access error occurs, if 
		 * <code>columnName</code> is not found or if the column value cannot be 
		 * converted to a valid SQL Date, Time or DateTime type
		 * @see SQLException.h
		 */
		virtual tm get_datetime(const char * column_name) override
		{
			struct tm tm = { 0 };
			int col_index = get_column_index(column_name);
			return ((col_index >= 0) ? get_datetime(col_index) : tm);
		}

	protected:

		void _ensure_capacity(int i)
		{
			if ((m_columns[i].length > m_bind[i].buffer_length))
			{
				/* Column was truncated, resize and fetch column directly. */
				std::realloc(m_columns[i].buffer, m_columns[i].length + 1);

				m_bind[i].buffer = m_columns[i].buffer;
				m_bind[i].buffer_length = m_columns[i].length;

				if ((mysql_util::MYSQL_OK != mysql_stmt_fetch_column(m_stmt, &m_bind[i], i, 0)))
					throw std::runtime_error(mysql_stmt_error(m_stmt));

				m_need_rebind = true;
			}
		}

		virtual void _init() override
		{

		}

	protected:

		MYSQL_STMT * m_stmt = nullptr;

		MYSQL_RES * m_meta = nullptr;

		std::unordered_map<std::string, int> m_column_name_map;

		MYSQL_BIND * m_bind = nullptr;

		mysql_util::column_t * m_columns = nullptr;

		int m_column_count = 0;

		bool m_need_rebind = false;

	};

}
