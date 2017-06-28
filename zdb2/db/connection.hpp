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
#include <atomic>
#include <chrono>
#include <stdexcept>

#include <zdb2/config.hpp>
#include <zdb2/net/url.hpp>
#include <zdb2/db/stmt.hpp>
#include <zdb2/db/resultset.hpp>

namespace zdb2
{

	class connection
	{
	public:
		connection(
			std::shared_ptr<url> url_ptr,
			std::size_t timeout = zdb2::DEFAULT_TIMEOUT
		)
			: m_url_ptr(url_ptr)
			, m_transaction(0)
			, m_timeout(timeout)
		{
		}

		virtual ~connection()
		{

		}


		/**
		 * Return the last time this Connection was accessed from the Connection Pool.
		 * The time is returned as the number of seconds since midnight, January 1, 
		 * 1970 GMT. Actions that your application takes, such as calling the public
		 * methods of this class does not affect this time.
		 * @param C A Connection object
		 * @return The last time (seconds) this Connection was accessed
		 */
		std::chrono::system_clock::time_point get_last_access_time()
		{
			return m_last_access_time;
		}


		/**
		 * Return true if this Connection is in a transaction that has not
		 * been committed.
		 * @param C A Connection object
		 * @return true if this Connection is in a transaction otherwise false
		 */
		bool is_intransaction()
		{
			return (m_transaction > 0);
		}


		//>> End Protected methods

		/** @name Properties */
		//@{

		/**
		 * Sets the number of milliseconds the Connection should wait for a
		 * SQL statement to finish if the database is busy. If the limit is
		 * exceeded, then the <code>execute</code> methods will return
		 * immediately with an error. The default timeout is <code>3
		 * seconds</code>.
		 * @param C A Connection object
		 * @param ms The query timeout limit in milliseconds; zero means
		 * there is no limit
		 */
		void set_query_timeout(std::size_t ms)
		{
			m_timeout = ms;
		}


		/**
		 * Retrieves the number of milliseconds the Connection will wait for a
		 * SQL statement object to execute.
		 * @param C A Connection object
		 * @return The query timeout limit in milliseconds; zero means there
		 * is no limit
		 */
		std::size_t get_query_timeout()
		{
			return m_timeout;
		}


		/**
		 * Returns this Connection URL
		 * @param C A Connection object
		 * @return This Connection URL
		 * @see URL.h
		 */
		std::shared_ptr<url> get_url()
		{
			return m_url_ptr;
		}

		//@}

		/**
		 * Ping the database server and returns true if this Connection is 
		 * alive, otherwise false in which case the Connection should be closed. 
		 * @param C A Connection object
		 * @return true if Connection is connected to a database server 
		 * otherwise false
		 */
		virtual bool ping() = 0;


		/**
		 * Close any ResultSet and PreparedStatements in the Connection. 
		 * Normally it is not necessary to call this method, but for some
		 * implementation (SQLite) it <i>may, in some situations,</i> be 
		 * necessary to call this method if a execution sequence error occurs.
		 * @param C A Connection object
		 */
		virtual void clear() = 0;


		/**
		 * Return connection to the connection pool. The same as calling 
		 * ConnectionPool_returnConnection() on a connection.
		 * @param C A Connection object
		 */
		virtual void close() = 0;


		/**
		 * Start a transaction. 
		 * @param C A Connection object
		 * @exception SQLException If a database error occurs
		 * @see SQLException.h
		 */
		virtual bool begin_transaction()
		{
			m_transaction++;
			return true;
		}


		/**
		 * Makes all changes made since the previous commit/rollback permanent
		 * and releases any database locks currently held by this Connection
		 * object.
		 * @param C A Connection object
		 * @exception SQLException If a database error occurs
		 * @see SQLException.h
		 */
		virtual bool commit()
		{
			m_transaction = 0;
			return true;
		}


		/**
		 * Undoes all changes made in the current transaction and releases any
		 * database locks currently held by this Connection object. This method
		 * will first call Connection_clear() before performing the rollback to
		 * clear any statements in progress such as selects.
		 * @param C A Connection object
		 * @exception SQLException If a database error occurs
		 * @see SQLException.h
		 */
		virtual bool rollback()
		{
			m_transaction = 0;
			return true;
		}


		/**
		 * Returns the value for the most recent INSERT statement into a 
		 * table with an AUTO_INCREMENT or INTEGER PRIMARY KEY column.
		 * @param C A Connection object
		 * @return The value of the rowid from the last insert statement
		 */
		virtual int64_t last_rowid() = 0;


		/**
		 * Returns the number of rows that was inserted, deleted or modified
		 * by the last Connection_execute() statement. If used with a
		 * transaction, this method should be called <i>before</i> commit is
		 * executed, otherwise 0 is returned.
		 * @param C A Connection object
		 * @return The number of rows changed by the last (DIM) SQL statement
		 */
		virtual int64_t rows_changed() = 0;


		/**
		 * Executes the given SQL statement, which may be an INSERT, UPDATE,
		 * or DELETE statement or an SQL statement that returns nothing, such
		 * as an SQL DDL statement. Several SQL statements can be used in the
		 * sql parameter string, each separated with the <i>;</i> SQL
		 * statement separator character. <b>Note</b>, calling this method
		 * clears any previous ResultSets associated with the Connection.
		 * @param C A Connection object
		 * @param sql A SQL statement
		 * @exception SQLException If a database error occurs. 
		 * @see SQLException.h
		 */
		virtual bool execute(const char * sql, ...) = 0;

		/**
		 * Executes the given SQL statement, which returns a single ResultSet
		 * object. You may <b>only</b> use one SQL statement with this method.
		 * This is different from the behavior of Connection_execute() which
		 * executes all SQL statements in its input string. If the sql
		 * parameter string contains more than one SQL statement, only the
		 * first statement is executed, the others are silently ignored.
		 * A ResultSet "lives" only until the next call to
		 * Connection_executeQuery(), Connection_execute() or until the 
		 * Connection is returned to the Connection Pool. <i>This means that 
		 * Result Sets cannot be saved between queries</i>.
		 * @param C A Connection object
		 * @param sql A SQL statement
		 * @return A ResultSet object that contains the data produced by the
		 * given query. 
		 * @exception SQLException If a database error occurs. 
		 * @see ResultSet.h
		 * @see SQLException.h
		 */
		virtual std::shared_ptr<resultset> query(const char *sql, ...) = 0;


		/**
		 * Creates a PreparedStatement object for sending parameterized SQL 
		 * statements to the database. The <code>sql</code> parameter may 
		 * contain IN parameter placeholders. An IN placeholder is specified 
		 * with a '?' character in the sql string. The placeholders are 
		 * then replaced with actual values by using the PreparedStatement's 
		 * setXXX methods. Only <i>one</i> SQL statement may be used in the sql 
		 * parameter, this in difference to Connection_execute() which may 
		 * take several statements. A PreparedStatement "lives" until the 
		 * Connection is returned to the Connection Pool. 
		 * @param C A Connection object
		 * @param sql A single SQL statement that may contain one or more '?' 
		 * IN parameter placeholders
		 * @return A new PreparedStatement object containing the pre-compiled
		 * SQL statement.
		 * @exception SQLException If a database error occurs. 
		 * @see PreparedStatement.h
		 * @see SQLException.h
		 */
		virtual std::shared_ptr<stmt> prepare_stmt(const char * sql, ...) = 0;


		/**
		 * This method can be used to obtain a string describing the last
		 * error that occurred. Inside a CATCH-block you can also find
		 * the error message directly in the variable Exception_frame.message.
		 * It is recommended to use this variable instead since it contains both
		 * SQL errors and API errors such as parameter index out of range etc, 
		 * while Connection_getLastError() might only show SQL errors
		 * @param C A Connection object
		 * @return A string explaining the last error
		 */
		virtual const char * get_last_error() = 0;


		/** @name Class methods */
		//@{

		/**
		 * <b>Class method</b>, test if the specified database system is 
		 * supported by this library. Clients may pass a full Connection URL, 
		 * for example using URL_toString(), or for convenience only the protocol
		 * part of the URL. E.g. "mysql" or "sqlite".
		 * @param url A database url string
		 * @return true if supported otherwise false
		 */
		virtual bool is_supported(const char *url) = 0;

		// @}

	protected:
		virtual bool _init() = 0;

		virtual bool _connect() = 0;

	protected:

		std::shared_ptr<url> m_url_ptr;

		std::size_t m_timeout  = zdb2::DEFAULT_TIMEOUT;

		int m_last_error = 0;

		std::atomic_int m_transaction;

		/// c++ 11 time,http://blog.csdn.net/oncealong/article/details/28599655
		std::chrono::system_clock::time_point m_last_access_time = std::chrono::system_clock::now();
	};

}
