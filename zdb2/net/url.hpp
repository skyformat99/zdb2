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
#include <cstring>
#include <string>
#include <memory>
#include <algorithm>
#include <stdexcept>
#include <unordered_map>

namespace zdb2 
{

	/**
	 * mysql
	 * mysql://localhost:3306/test?user=root&password=swordfish
	 * 
	 * sqlite
	 * sqlite:///var/sqlite/test.db?synchronous=normal&heap_limit=8000&foreign_keys=on
	 * 
	 * postgresql
	 * postgresql://localhost:5432/test?user=root&password=swordfish
	 * 
	 * oracle
	 * oracle://localhost:1521/test?user=scott&password=tiger
	 * oracle:///servicename?user=scott&password=tiger
	 *
	 * sqlserver
	 * sqlserver://localhost:3306/test?user=root&password=swordfish
	 */
	class url
	{
	public:
		url(const char * url_string)
		{
			if (!_parse_url(url_string))
			{
				_clear();
			}
		}
		virtual ~url()
		{
		}

		std::string get_host()   { return m_host; }
		std::string get_dbtype() { return m_dbtype; }
		std::string get_dbname() { return m_dbname; }
		std::string get_port()   { return m_port; }

		std::string get_param_value(std::string name)
		{
			auto iterator = m_params.find(name);
			if (iterator != m_params.end())
			{
				return iterator->second;
			}
			return "";
		}

		template<typename _handler>
		void for_each_param(_handler h)
		{
			std::for_each(m_params.begin(), m_params.end(), h);
		}

	protected:

		bool _parse_url(const char * url_string)
		{
			if (!url_string || *url_string == '\0')
			{
				throw std::runtime_error("url string is null.");
				return false;
			}

			m_url = url_string;

			// erase the invalid character of head,eg : space \t \n and so on 
			while (!((m_url[0] >= 'a' && m_url[0] <= 'z') || (m_url[0] >= 'A' && m_url[0] <= 'Z')))
				m_url.erase(0, 1);

			// parse the database type
			std::size_t pos_dbtype_end = m_url.find_first_of("://", 0);
			if (pos_dbtype_end == 0 || pos_dbtype_end == std::string::npos)
			{
				throw std::runtime_error("url string is invalid,can't parse the database type.");
				return false;
			}
			m_dbtype = m_url.substr(0, pos_dbtype_end - 0);

			std::transform(m_dbtype.begin(), m_dbtype.end(), m_dbtype.begin(), ::tolower);

			std::size_t pos_host_begin = pos_dbtype_end + std::strlen("://");

			if (m_url.length() <= pos_host_begin)
			{
				throw std::runtime_error("url string is invalid,no database type specified in url.");
				return false;
			}

			if (m_dbtype == "mysql")
				return _parse_mysql(pos_host_begin);
			else if (m_dbtype == "oracle")
				return _parse_oracle(pos_host_begin);
			else if (m_dbtype == "postgresql")
				return _parse_postgresql(pos_host_begin);
			else if (m_dbtype == "sqlite")
				return _parse_sqlite(pos_host_begin);
			else if (m_dbtype == "sqlserver")
				return _parse_sqlserver(pos_host_begin);
			else
				throw std::runtime_error("unknown database type.");

			return false;
		}

		// mysql://localhost:3306/test?user=root&password=swordfish
		bool _parse_mysql(std::size_t pos_host_begin)
		{
			return _parse_standard(pos_host_begin);
		}

		// oracle://localhost:1521/test?user=scott&password=tiger
		bool _parse_oracle(std::size_t pos_host_begin)
		{
			if (m_url[pos_host_begin] == '/')
			{
				pos_host_begin++;
				return _parse_oracle_by_service(pos_host_begin);
			}

			return _parse_standard(pos_host_begin);
		}

		// postgresql://localhost:5432/test?user=root&password=swordfish
		bool _parse_postgresql(std::size_t pos_host_begin)
		{
			return _parse_standard(pos_host_begin);
		}

		// sqlite:///var/sqlite/test.db?synchronous=normal&heap_limit=8000&foreign_keys=on
		bool _parse_sqlite(std::size_t pos_host_begin)
		{
			// parse the database file name
			std::size_t pos_db_end = m_url.find_first_of('?', pos_host_begin);
			if (pos_db_end == pos_host_begin || pos_db_end == std::string::npos)
			{
				throw std::runtime_error("url string is invalid,no sqlite database file specified in url.");
				return false;
			}
			m_dbname = m_url.substr(pos_host_begin, pos_db_end - pos_host_begin);

			pos_db_end++;

			return _parse_params(pos_db_end);
		}

		// sqlserver://localhost:3306/test?user=root&password=swordfish
		bool _parse_sqlserver(std::size_t pos_host_begin)
		{
			return _parse_standard(pos_host_begin);
		}

		bool _parse_standard(std::size_t pos_host_begin)
		{
			// parse the host
			std::size_t pos_host_end = m_url.find_first_of(':', pos_host_begin);
			if (pos_host_end == pos_host_begin || pos_host_end == std::string::npos)
			{
				throw std::runtime_error("url string is invalid,no host specified in url.");
				return false;
			}

			m_host = m_url.substr(pos_host_begin, pos_host_end - pos_host_begin);

			pos_host_end++;

			// parse the port
			std::size_t pos_port_end = m_url.find_first_of('/', pos_host_end);
			if (pos_port_end == pos_host_end || pos_port_end == std::string::npos)
			{
				throw std::runtime_error("url string is invalid,no port specified in url.");
				return false;
			}
			m_port = m_url.substr(pos_host_end, pos_port_end - pos_host_end);

			pos_port_end++;

			// parse the database name
			std::size_t pos_db_end = m_url.find_first_of('?', pos_port_end);
			if (pos_db_end == pos_port_end || pos_db_end == std::string::npos)
			{
				throw std::runtime_error("url string is invalid,no database name specified in url.");
				return false;
			}
			m_dbname = m_url.substr(pos_port_end, pos_db_end - pos_port_end);

			pos_db_end++;

			// parse all other params
			return _parse_params(pos_db_end);
		}

		bool _parse_params(std::size_t pos_params_begin)
		{
			if (m_url.length() <= pos_params_begin)
				return false;

			std::string params = m_url.substr(pos_params_begin);
			if (params[params.length() - 1] != '&')
				params += '&';

			std::size_t pos_sep = 0, pos_head = 0;
			pos_sep = params.find_first_of('&', pos_sep);
			while (pos_sep > pos_head && pos_sep != std::string::npos)
			{
				std::string param = params.substr(pos_head, pos_sep - pos_head);

				std::size_t pos_equal = param.find_first_of('=', 0);
				if (pos_equal > 0 && pos_equal + 1 < param.length())
				{
					std::string name = param.substr(0, pos_equal);
					pos_equal++;
					std::string value = param.substr(pos_equal);
					m_params.emplace(name, value);
				}

				pos_sep++;
				pos_head = pos_sep;

				pos_sep = params.find_first_of('&', pos_sep);
			}

			return true;
		}

		// oracle:///servicename?user=scott&password=tiger
		bool _parse_oracle_by_service(std::size_t pos_service_begin)
		{
			// parse the service name
			std::size_t pos_service_end = m_url.find_first_of('?', pos_service_begin);
			if (pos_service_end == pos_service_begin || pos_service_end == std::string::npos)
			{
				throw std::runtime_error("url string is invalid,no oracle service name specified in url.");
				return false;
			}

			m_host = m_url.substr(pos_service_begin, pos_service_end - pos_service_begin);

			pos_service_end++;

			// parse all other params
			return _parse_params(pos_service_end);
		}

	protected:

		std::string m_url;

		std::string m_host;
		std::string m_dbtype;
		std::string m_dbname;
		std::string m_port;

		std::unordered_map<std::string, std::string> m_params;

		void _clear()
		{
			m_url.clear();

			m_host.clear();
			m_dbtype.clear();
			m_dbname.clear();
			m_port.clear();

			m_params.clear();
		}

	};

}
