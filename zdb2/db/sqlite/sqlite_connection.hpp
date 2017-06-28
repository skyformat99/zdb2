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
#include <functional>

#include <sqlite3.h>

#include <zdb2/config.hpp>
#include <zdb2/net/url.hpp>
#include <zdb2/db/connection.hpp>

#include <zdb2/db/sqlite/sqlite_util.hpp>
#include <zdb2/db/sqlite/sqlite_stmt.hpp>
#include <zdb2/db/sqlite/sqlite_resultset.hpp>

namespace zdb2
{

	class sqlite_connection : public connection
	{
	public:
		sqlite_connection(
			std::shared_ptr<url> url_ptr,
			std::size_t timeout = zdb2::DEFAULT_TIMEOUT
		)
			: connection(url_ptr, timeout)
		{
			_init();
		}

		virtual ~sqlite_connection()
		{

		}

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
			connection::set_query_timeout(ms);

			if(m_db)
				sqlite3_busy_timeout(m_db, (int)m_timeout);
		}

		//@}

		/**
		 * Ping the database server and returns true if this Connection is 
		 * alive, otherwise false in which case the Connection should be closed. 
		 * @param C A Connection object
		 * @return true if Connection is connected to a database server 
		 * otherwise false
		 */
		virtual bool ping() override
		{
			return (SQLITE_OK == _execute_sql("select 1;"));
		}


		/**
		 * Close any ResultSet and PreparedStatements in the Connection. 
		 * Normally it is not necessary to call this method, but for some
		 * implementation (SQLite) it <i>may, in some situations,</i> be 
		 * necessary to call this method if a execution sequence error occurs.
		 * @param C A Connection object
		 */
		virtual void clear() override
		{

		}


		/**
		 * Return connection to the connection pool. The same as calling 
		 * ConnectionPool_returnConnection() on a connection.
		 * @param C A Connection object
		 */
		virtual void close() override
		{
			if (m_db)
			{
				while (sqlite3_close(m_db) == SQLITE_BUSY)
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				m_db = nullptr;
			}
		}


		/**
		 * Start a transaction. 
		 * @param C A Connection object
		 * @exception SQLException If a database error occurs
		 * @see SQLException.h
		 */
		virtual bool begin_transaction() override
		{
			if (SQLITE_OK == _execute_sql("BEGIN TRANSACTION;"))
				return connection::begin_transaction();
			return false;
		}


		/**
		 * Makes all changes made since the previous commit/rollback permanent
		 * and releases any database locks currently held by this Connection
		 * object.
		 * @param C A Connection object
		 * @exception SQLException If a database error occurs
		 * @see SQLException.h
		 */
		virtual bool commit() override
		{
			if (is_intransaction())
			{
				if (connection::commit())
				{
					return (SQLITE_OK == _execute_sql("COMMIT TRANSACTION;"));
				}
			}
			return false;
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
		virtual bool rollback() override
		{
			if (is_intransaction())
			{
				if (connection::rollback())
				{
					return (SQLITE_OK == _execute_sql("ROLLBACK TRANSACTION;"));
				}
			}
			return false;
		}


		/**
		 * Returns the value for the most recent INSERT statement into a 
		 * table with an AUTO_INCREMENT or INTEGER PRIMARY KEY column.
		 * @param C A Connection object
		 * @return The value of the rowid from the last insert statement
		 */
		virtual int64_t last_rowid() override
		{
			return (m_db ? sqlite3_last_insert_rowid(m_db) : 0);
		}


		/**
		 * Returns the number of rows that was inserted, deleted or modified
		 * by the last Connection_execute() statement. If used with a
		 * transaction, this method should be called <i>before</i> commit is
		 * executed, otherwise 0 is returned.
		 * @param C A Connection object
		 * @return The number of rows changed by the last (DIM) SQL statement
		 */
		virtual int64_t rows_changed() override
		{
			return (m_db ? (int64_t)sqlite3_changes(m_db) : 0);
		}


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
		virtual bool execute(const char * sql, ...) override
		{
			if (!sql || sql[0] == '\0')
				return false;

			va_list ap, ap_copy;
			va_start(ap, sql);
			
			va_copy(ap_copy, ap);
			int len = std::vsnprintf(nullptr, 0, sql, ap_copy);

			std::string str(len, '\0');

			va_copy(ap_copy, ap);
			std::vsprintf((char*)str.data(), sql, ap_copy);

			va_end(ap);
			
			return (_execute_sql(str.c_str()) == SQLITE_OK);
		}

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
		virtual std::shared_ptr<resultset> query(const char *sql, ...)
		{
			if (!sql || sql[0] == '\0')
				return nullptr;

			va_list ap, ap_copy;
			va_start(ap, sql);

			va_copy(ap_copy, ap);
			int len = std::vsnprintf(nullptr, 0, sql, ap_copy);

			std::string str(len, '\0');

			va_copy(ap_copy, ap);
			std::vsprintf((char*)str.data(), sql, ap_copy);

			va_end(ap);

			int status;
			const char * tail;
			sqlite3_stmt * stmt;

#if defined SQLITEUNLOCK && SQLITE_VERSION_NUMBER >= 3006012
			status = sqlite_util::sqlite3_blocking_prepare_v2(m_db, str.c_str(), (int)str.length(), &stmt, &tail);
#elif SQLITE_VERSION_NUMBER >= 3004000
			status = sqlite_util::execute(m_timeout, sqlite3_prepare_v2, m_db, str.c_str(), (int)str.length(), &stmt, &tail);
#else
			status = sqlite_util::execute(m_timeout, sqlite3_prepare, m_db, str.c_str(), (int)str.length(), &stmt, &tail);
#endif
			if (status == SQLITE_OK)
				return std::dynamic_pointer_cast<resultset>(std::make_shared<sqlite_resultset>(stmt,m_timeout));

			return nullptr;
		}

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
		virtual std::shared_ptr<stmt> prepare_stmt(const char * sql, ...) override
		{
			if (!sql || sql[0] == '\0')
				return nullptr;

			va_list ap, ap_copy;
			va_start(ap, sql);

			va_copy(ap_copy, ap);
			int len = std::vsnprintf(nullptr, 0, sql, ap_copy);

			std::string str(len, '\0');

			va_copy(ap_copy, ap);
			std::vsprintf((char*)str.data(), sql, ap_copy);

			va_end(ap);

			return std::dynamic_pointer_cast<stmt>(std::make_shared<sqlite_stmt>(m_db,str.c_str(),m_timeout));
		}


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
		virtual const char * get_last_error() override
		{
			return sqlite3_errmsg(m_db);
		}


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
		virtual bool is_supported(const char *url) override
		{
			return true;
		}

		// @}

	protected:
		virtual bool _init() override
		{
			if (!_connect())
				return false;
 
			// There is no PRAGMA for heap limit as of sqlite-3.7.0, so we make it a configurable property using "heap_limit" [kB]
			std::string heap_limit = m_url_ptr->get_param_value("heap_limit");
			if (!heap_limit.empty())
			{
#if defined(HAVE_SQLITE3_SOFT_HEAP_LIMIT64)
				sqlite3_soft_heap_limit64(Str_parseInt(URL_getParameter(C->url, properties[i])) * 1024);
#elif defined(HAVE_SQLITE3_SOFT_HEAP_LIMIT)
				sqlite3_soft_heap_limit(Str_parseInt(URL_getParameter(C->url, properties[i])) * 1024);
#endif
			}

			// parse other params 
			std::string params;
			m_url_ptr->for_each_param([&params](std::pair<std::string, std::string> pair)
			{
				if (!pair.first.empty() && !pair.second.empty() && pair.first != "heap_limit")
				{
					params += "PRAGMA ";
					params += pair.first;
					params += " = ";
					params += pair.second;
					params += "; ";
				}
			});

			// execute extra params
			if (!params.empty())
			{
				if (SQLITE_OK != _execute_sql(params.c_str()))
				{
					close();
					throw std::runtime_error("error : unable to set database pragmas.");
					return false;
				}
			}

			return true;
		}

		virtual bool _connect() override
		{
			int status;
			std::string path = m_url_ptr->get_dbname();
			if (path.empty())
			{
				throw std::runtime_error("error : no database specified in url");
				return false;
			}
			/* Shared cache mode help reduce database lock problems if libzdb is used with many threads */
#if SQLITE_VERSION_NUMBER >= 3005000
#ifndef DARWIN
			/*
			SQLite doc e.al.: "sqlite3_enable_shared_cache is disabled on MacOS X 10.7 and iOS version 5.0 and
			will always return SQLITE_MISUSE. On those systems, shared cache mode should be enabled
			per-database connection via sqlite3_open_v2() with SQLITE_OPEN_SHAREDCACHE".
			As of OS X 10.10.4 this method is still deprecated and it is unclear if the recomendation above
			holds as SQLite from 3.5 requires that both sqlite3_enable_shared_cache() _and_
			sqlite3_open_v2(SQLITE_OPEN_SHAREDCACHE) is used to enable shared cache (!).
			*/
			sqlite3_enable_shared_cache(true);
#endif
			status = sqlite3_open_v2(path.c_str(), &m_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_SHAREDCACHE, NULL);
#else
			status = sqlite3_open(path.c_str(), &m_db);
#endif
			if (SQLITE_OK != status)
			{
				throw std::runtime_error("error : cannot open database,check if the database file exists.");
				sqlite3_close(m_db);
				return false;
			}
			return true;
		}


		int _execute_sql(const char * sql)
		{
#if defined SQLITEUNLOCK && SQLITE_VERSION_NUMBER >= 3006012
			return sqlite_util::sqlite3_blocking_exec(m_db, sql, nullptr, nullptr, nullptr);
#else
			return sqlite_util::execute(m_timeout, sqlite3_exec, m_db, sql, nullptr, nullptr, nullptr);
#endif
		}

		sqlite3 * m_db = nullptr;
	};

}
