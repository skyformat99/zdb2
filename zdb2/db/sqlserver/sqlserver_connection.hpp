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

#if defined(__unix__) || defined(__linux__)

#elif defined(WIN32) || defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS_)
#include <windows.h>
#endif


#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <odbcss.h>

#include <zdb2/config.hpp>
#include <zdb2/net/url.hpp>
#include <zdb2/db/connection.hpp>

#include <zdb2/db/sqlserver/sqlserver_util.hpp>
#include <zdb2/db/sqlserver/sqlserver_stmt.hpp>
#include <zdb2/db/sqlserver/sqlserver_resultset.hpp>

namespace zdb2
{

	class sqlserver_connection : public connection
	{
	public:
		sqlserver_connection(
			std::shared_ptr<url> url_ptr,
			std::size_t timeout = zdb2::DEFAULT_TIMEOUT
		)
			: connection(url_ptr, timeout)
		{
			_init();
		}

		virtual ~sqlserver_connection()
		{
			close();
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
			int status = _execute_sql("select 1;");
			return ((status == SQL_SUCCESS) || (status == SQL_SUCCESS_WITH_INFO));
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
			if (m_hdbc)
			{
				SQLDisconnect(m_hdbc);
				SQLFreeHandle(SQL_HANDLE_DBC, m_hdbc);
				m_hdbc = nullptr;
			}
			if (m_henv)
			{
				SQLFreeHandle(SQL_HANDLE_ENV, m_henv);
				m_henv = nullptr;
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
			int status = _execute_sql("BEGIN TRANSACTION;");
			if ((status == SQL_SUCCESS) || (status == SQL_SUCCESS_WITH_INFO))
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
					int status = _execute_sql("COMMIT TRANSACTION;");
					return ((status == SQL_SUCCESS) || (status == SQL_SUCCESS_WITH_INFO));
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
					int status = _execute_sql("ROLLBACK TRANSACTION;");
					return ((status == SQL_SUCCESS) || (status == SQL_SUCCESS_WITH_INFO));
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
			int status = _execute_sql("select scope_identity()");
			return ((status == SQL_SUCCESS) || (status == SQL_SUCCESS_WITH_INFO));
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
			return 0;
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
			
			int status = _execute_sql(str.c_str());
			return ((status == SQL_SUCCESS) || (status == SQL_SUCCESS_WITH_INFO));
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

			//int status;
			//const char * tail;
			//sqlite3_stmt * stmt;
//
//#if defined SQLITEUNLOCK && SQLITE_VERSION_NUMBER >= 3006012
//			status = sqlite_util::sqlite3_blocking_prepare_v2(m_db, str.c_str(), (int)str.length(), &stmt, &tail);
//#elif SQLITE_VERSION_NUMBER >= 3004000
//			status = sqlite_util::execute(m_timeout, sqlite3_prepare_v2, m_db, str.c_str(), (int)str.length(), &stmt, &tail);
//#else
//			status = sqlite_util::execute(m_timeout, sqlite3_prepare, m_db, str.c_str(), (int)str.length(), &stmt, &tail);
//#endif
			//if (status == SQLITE_OK)
			//	return std::dynamic_pointer_cast<resultset>(std::make_shared<sqlite_resultset>(stmt,m_timeout));

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

			return nullptr;

			//return std::dynamic_pointer_cast<stmt>(std::make_shared<sqlite_stmt>(m_db,str.c_str(),m_timeout));
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
			//unsigned char szSQLSTATE[10];
			//SDWORD nErr;
			//unsigned char msg[SQL_MAX_MESSAGE_LENGTH + 1];
			//SWORD cbmsg;

			//unsigned char szData[256];   // Returned data storage

			//while (SQLError(0, 0, hstmt, szSQLSTATE, &nErr, msg, sizeof(msg), &cbmsg) ==SQL_SUCCESS)
			//{
			//	snprintf((char *)szData, sizeof(szData), "Error:\nSQLSTATE=%s,Native error=%ld,msg='%s'", szSQLSTATE, nErr, msg);
			//	//MessageBox(NULL,(const char *)szData,"ODBC Error",MB_OK);
			//	//return NULL;
			//	//return strdup(szData);
			//}
			return "";
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
					throw std::runtime_error("unable to set database pragmas.");
					return false;
				}
			}

			return true;
		}

		virtual bool _connect() override
		{
			if (m_hdbc)
			{
				SQLFreeHandle(SQL_HANDLE_DBC, m_hdbc);
				m_hdbc = nullptr;
			}
			if (m_henv)
			{
				SQLFreeHandle(SQL_HANDLE_ENV, m_henv);
				m_henv = nullptr;
			}

			RETCODE retcode;

			// 1.alloc env
			//retcode = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &m_henv);
			//retcode = SQLSetEnvAttr(m_henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_INTEGER);
			retcode = SQLAllocEnv(&m_henv);

			// 2.connect
			//retcode = SQLAllocHandle(SQL_HANDLE_DBC, m_henv, &m_hdbc);
			retcode = SQLAllocConnect(m_henv, &m_hdbc);
			retcode = SQLSetConnectAttr(m_hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER *)5, 0);

			std::string user = m_url_ptr->get_param_value("user");
			std::string pass = m_url_ptr->get_param_value("password");
			if (user.empty() || pass.empty())
			{
				throw std::runtime_error("url string is invalid,can't find the user and password parameters.");
				return false;
			}

			retcode = SQLConnect(
				m_hdbc,
				(SQLCHAR *)m_url_ptr->get_dbname().c_str(),
				SQL_NTS,
				(SQLCHAR *)user.c_str(),
				SQL_NTS,
				(SQLCHAR *)pass.c_str(),
				SQL_NTS
			);

			if ((retcode == SQL_SUCCESS) || (retcode == SQL_SUCCESS_WITH_INFO))
			{
				return true;
			}

			return false;
		}


		int _execute_sql(const char * sql)
		{
			SQLHSTMT hstmt;
			SQLAllocStmt(m_hdbc, &hstmt);
			int status = SQLExecDirect(hstmt, (SQLCHAR *)sql, SQL_NTS);
			SQLFreeStmt(hstmt, SQL_DROP);
			return status;
		}

		SQLHENV m_henv = nullptr;
		SQLHDBC m_hdbc = nullptr;
	};

}
