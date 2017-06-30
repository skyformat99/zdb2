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
#include <condition_variable>
#include <thread>
#include <deque>
#include <stdexcept>

#include <zdb2/config.hpp>

#include <zdb2/net/url.hpp>
#include <zdb2/util/spin_lock.hpp>

#include <zdb2/db/connection.hpp>
#include <zdb2/db/sqlite/sqlite_connection.hpp>
#include <zdb2/db/mysql/mysql_connection.hpp>
#include <zdb2/db/sqlserver/sqlserver_connection.hpp>

namespace zdb2 
{

	class pool : public std::enable_shared_from_this<pool>
	{
	public:
		pool(
			std::shared_ptr<url> url_ptr,
			std::size_t init_conn_count = zdb2::DEFAULT_INIT_CONNECTIONS,
			std::size_t conn_timeout    = zdb2::DEFAULT_CONNECTION_TIMEOUT,
			std::size_t execute_timeout = zdb2::DEFAULT_TIMEOUT,
			std::size_t max_conn_count  = zdb2::DEFAULT_MAX_CONNECTIONS,
			std::size_t sweep_interval  = zdb2::DEFAULT_SWEEP_INTERVAL
		)
			: m_url_ptr(url_ptr)
			, m_init_conn_count(init_conn_count)
			, m_conn_timeout(conn_timeout)
			, m_execute_timeout(execute_timeout)
			, m_max_conn_count(max_conn_count)
			, m_sweep_interval(sweep_interval)
		{
			_init();
		}

		virtual ~pool()
		{
			destroy();
			// check whether all connection is not using.
			assert(m_using_count == 0);
		}

		std::shared_ptr<url> get_url()
		{
			return m_url_ptr;
		}

		std::shared_ptr<connection> get()
		{
			std::lock_guard<spin_lock> g(m_lock);

			// [important] : 
			// if we make this_ptr by shared_from_this and passed it to the lumbda function,and the lumbda function
			// is as the shared_ptr<connection> custom deleter,we must insure that the class connection is not derived
			// from std::enable_shared_from_this,otherwise when application exit,the pool shared_ptr reference count
			// will not desired to 0,so the pool destructor will not be called,and will cause memory leaks.Why does 
			// this happen? i find that if we delete the connection pointer in the lumbda,this problem will not happen,
			// but we can't delete the connection pointer in the lumbda under this design.
			// [important] :
			// why pass the this_ptr by shared_from_this to the lumdba function ? why not pass "this" pointer to the
			// lumdba function directly?because the connection shared_ptr custom deleter has used "this" pool object,
			// when the connection shared_ptr is destructed,it will call the custom deleter,but at this time the "this" 
			// pool object may be destructed already before the connection shared_ptr destructed,this will cause crash,
			// so pass a this_ptr by shared_from_this to the custom deleter,can make sure the "this" pool obejct is 
			// destructed after the the connection shared_ptr destructed.
			auto this_ptr = this->shared_from_this();
			auto deleter = [this_ptr](connection * conn)
			{
				std::lock_guard<spin_lock> g(this_ptr->m_lock);
				this_ptr->m_connections.emplace_back(conn);
				this_ptr->m_using_count--;
			};

			if (m_connections.size() > 0)
			{
				auto conn = m_connections.front();
				m_connections.pop_front();

				m_using_count++;

				return std::shared_ptr<connection>(conn, deleter);
			}

			if (m_using_count < m_max_conn_count)
			{
				connection * conn = new_connection();
				if (conn)
				{
					m_using_count++;

					return std::shared_ptr<connection>(conn, deleter);
				}
			}

			return nullptr;
		}

		void destroy()
		{
			if (m_sweep_thread_ptr && m_sweep_thread_ptr->joinable())
			{
				{
					std::unique_lock<std::mutex> lck(m_mtx);
					m_stopped = true;
					m_cv.notify_all();
				}

				m_sweep_thread_ptr->join();
			}
			if (m_connections.size() > 0)
			{
				std::lock_guard<spin_lock> g(m_lock);

				for (auto & conn : m_connections)
				{
					delete conn;
				}

				m_connections.clear();
			}
		}

	protected:
		bool _init()
		{
			std::lock_guard<spin_lock> g(m_lock);

			for (std::size_t i = 0; i < m_init_conn_count; i++)
			{
				connection * conn = new_connection();
				if (conn)
				{
					m_connections.emplace_back(conn);
				}
			}

			if (m_connections.size() == 0)
			{
				throw std::runtime_error("failed to fill the pool with initial connections.");
				return false;
			}

			m_sweep_thread_ptr = std::make_shared<std::thread>(std::bind(&pool::_sweep_func, this));

			return true;
		}

		void _sweep_func()
		{
			while (!m_stopped)
			{
				{
					std::unique_lock <std::mutex> lck(m_mtx);
					m_cv.wait_for(lck, std::chrono::seconds(m_sweep_interval));
				}

				if (m_stopped)
					break;

				_reap_connections();
			}
		}

		void _reap_connections()
		{
			if (m_connections.size() > 0)
			{
				std::lock_guard<spin_lock> g(m_lock);

				for (auto begin = m_connections.begin(); begin != m_connections.end();)
				{
					auto time_diff = std::chrono::system_clock::now() - (*begin)->get_last_access_time();
					auto seconds = std::chrono::duration_cast<std::chrono::seconds>(time_diff).count();
					if ((std::size_t)seconds > m_conn_timeout || !(*begin)->ping())
					{
						delete (*begin);

						// when erase a elem,the iterator will auto point to the next element
						begin = m_connections.erase(begin);
					}
					else
					{
						begin++;
					}
				}
			}
		}

		connection * new_connection()
		{
			std::string _db_type = m_url_ptr->get_dbtype();
			if (_db_type == "mysql")
				return dynamic_cast<connection *>(new mysql_connection(m_url_ptr, m_execute_timeout));
			else if (_db_type == "oracle")
				return dynamic_cast<connection *>(new sqlite_connection(m_url_ptr, m_execute_timeout));
			else if (_db_type == "postgresql")
				return dynamic_cast<connection *>(new sqlite_connection(m_url_ptr, m_execute_timeout));
			else if (_db_type == "sqlite")
				return dynamic_cast<connection *>(new sqlite_connection(m_url_ptr, m_execute_timeout));
			else if (_db_type == "sqlserver")
				return dynamic_cast<connection *>(new sqlserver_connection(m_url_ptr, m_execute_timeout));
			else
				throw std::runtime_error("unknown database type.");
			return nullptr;
		}

	protected:

		std::shared_ptr<url> m_url_ptr;

		/// lock used to insure pool multi thread safe
		spin_lock m_lock;

		/// below three members used to safe destroy the pool and exit
		volatile bool m_stopped = false;
		std::mutex m_mtx;
		std::condition_variable m_cv;

		/// the thread shared_ptr of reap the connections
		std::shared_ptr<std::thread> m_sweep_thread_ptr;

		/// idle count of connections 
		std::deque<connection *> m_connections;

		/// using count of connections
		std::size_t m_using_count = 0;

		std::size_t m_init_conn_count = zdb2::DEFAULT_INIT_CONNECTIONS;
		std::size_t m_conn_timeout    = zdb2::DEFAULT_CONNECTION_TIMEOUT;
		std::size_t m_execute_timeout = zdb2::DEFAULT_TIMEOUT;
		std::size_t m_max_conn_count  = zdb2::DEFAULT_MAX_CONNECTIONS;
		std::size_t m_sweep_interval  = zdb2::DEFAULT_SWEEP_INTERVAL;

	};

}
