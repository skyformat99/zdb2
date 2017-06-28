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

#include <libpq-fe.h>

#include <zdb2/config.hpp>
#include <zdb2/net/url.hpp>
#include <zdb2/db/connection.hpp>

#include <zdb2/db/postgresql/postgresql_util.hpp>
#include <zdb2/db/postgresql/postgresql_stmt.hpp>
#include <zdb2/db/postgresql/postgresql_resultset.hpp>

namespace zdb2
{

	class postgresql_connection : public connection
	{
	public:
		postgresql_connection(
			std::shared_ptr<url> url_ptr,
			std::size_t timeout = zdb2::DEFAULT_TIMEOUT
		)
			: connection(url_ptr, timeout)
		{
			_init();
		}

		virtual ~postgresql_connection()
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
			return (m_db ? (mysql_ping(m_db) == mysql_util::MYSQL_OK) : false);
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
				mysql_close(m_db);
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
			if (m_db)
			{
				if (mysql_util::MYSQL_OK == mysql_query(m_db, "START TRANSACTION;"))
					return connection::begin_transaction();
			}
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
			if (m_db)
			{
				if (is_intransaction())
				{
					if (connection::commit())
					{
						return (mysql_util::MYSQL_OK == mysql_query(m_db, "COMMIT;"));
					}
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
			if (m_db)
			{
				if (is_intransaction())
				{
					if (connection::rollback())
					{
						return (mysql_util::MYSQL_OK == mysql_query(m_db, "ROLLBACK;"));
					}
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
			return (m_db ? (int64_t)mysql_insert_id(m_db) : 0);
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
			return (m_db ? (int64_t)mysql_affected_rows(m_db) : 0);
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
			
			return (mysql_util::MYSQL_OK == mysql_real_query(m_db, str.c_str(), (unsigned long)str.length()));
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
		virtual std::shared_ptr<resultset> query(const char *sql, ...) override
		{
			if (!m_db || !sql || sql[0] == '\0')
				return nullptr;

			va_list ap, ap_copy;
			va_start(ap, sql);

			va_copy(ap_copy, ap);
			int len = std::vsnprintf(nullptr, 0, sql, ap_copy);

			std::string str(len, '\0');

			va_copy(ap_copy, ap);
			std::vsprintf((char*)str.data(), sql, ap_copy);

			va_end(ap);

			MYSQL_STMT * stmt = mysql_stmt_init(m_db);
			if (!stmt)
				return nullptr;

			if (mysql_util::MYSQL_OK != mysql_stmt_prepare(stmt, str.c_str(), (unsigned long)str.length()))
			{
				mysql_stmt_close(stmt);
				return nullptr;
			}

#if MYSQL_VERSION_ID >= 50002
			unsigned long cursor = CURSOR_TYPE_READ_ONLY;
			mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, &cursor);
#endif

			if ((mysql_util::MYSQL_OK != mysql_stmt_execute(stmt)))
			{
				mysql_stmt_close(stmt);
				return nullptr;
			}

			return std::dynamic_pointer_cast<resultset>(std::make_shared<mysql_resultset>(stmt, m_timeout));
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

			return std::dynamic_pointer_cast<stmt>(std::make_shared<mysql_stmt>(m_db,str.c_str(),m_timeout));
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
			return mysql_error(m_db);
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
			return _connect();
		}

		virtual bool _connect() override
		{
			unsigned long client_flags = CLIENT_MULTI_STATEMENTS;
			int connect_timeout = zdb2::DEFAULT_TCP_TIMEOUT;

			std::string unix_socket = m_url_ptr->get_param_value("unix-socket");
			std::string user = m_url_ptr->get_param_value("user");
			std::string pass = m_url_ptr->get_param_value("password");
			std::string host = m_url_ptr->get_host();
			std::string port = m_url_ptr->get_port();
			std::string database = m_url_ptr->get_dbname();
			std::string timeout = m_url_ptr->get_param_value("connect-timeout");
			std::string charset = m_url_ptr->get_param_value("charset");

			if (!unix_socket.empty())
			{
				host = "localhost"; // Make sure host is localhost if unix socket is to be used
			}
			else if (host.empty())
			{
				throw std::runtime_error("error : no host specified in url.");
				return false;
			}
			if (port.empty())
			{
				throw std::runtime_error("error : no port specified in url.");
				return false;
			}
			if (database.empty())
			{
				throw std::runtime_error("error : no database specified in url.");
				return false;
			}
			if (user.empty())
			{
				throw std::runtime_error("error : no username specified in url.");
				return false;
			}
			if (pass.empty())
			{
				throw std::runtime_error("error : no password specified in url.");
				return false;
			}

			m_db = mysql_init(nullptr);
			if (!m_db)
			{
				throw std::runtime_error("error : unable to allocate mysql handler.");
				return false;
			}
			
			/* Options */
			if (m_url_ptr->get_param_value("compress") == "true")
				client_flags |= CLIENT_COMPRESS;

			if (m_url_ptr->get_param_value("use-ssl") == "true")
				mysql_ssl_set(m_db, nullptr, nullptr, nullptr, nullptr, nullptr);

			if (m_url_ptr->get_param_value("secure-auth") == "true")
				mysql_options(m_db, MYSQL_SECURE_AUTH, (const void*)&mysql_util::yes);
			else
				mysql_options(m_db, MYSQL_SECURE_AUTH, (const void*)&mysql_util::no);

			if (!timeout.empty())
			{
				int _timeout = std::atoi(timeout.c_str());
				if (_timeout > 0)
					connect_timeout = _timeout;
			}
			mysql_options(m_db, MYSQL_OPT_CONNECT_TIMEOUT, (const void *)&connect_timeout);

			if (!charset.empty())
				mysql_options(m_db, MYSQL_SET_CHARSET_NAME, (const void *)charset.c_str());

#if MYSQL_VERSION_ID >= 50013
			mysql_options(m_db, MYSQL_OPT_RECONNECT, (const void*)&mysql_util::yes);
#endif
			/* Connect */
			if (mysql_real_connect(m_db, host.c_str(), user.c_str(), pass.c_str(), database.c_str(), (unsigned int)std::atoi(port.c_str()), unix_socket.c_str(), client_flags))
				return true;

			mysql_close(m_db);

			return false;
		}

	protected:

		OCIEnv*        m_env;
		OCIError*      m_err;
		OCISvcCtx*     m_svc;
		OCISession*    m_usr;
		OCIServer*     m_srv;
		OCITrans*      m_txnhp;
		char           m_erb[ERB_SIZE];

	};

}
