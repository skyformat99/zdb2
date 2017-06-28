/*
 * COPYRIGHT (C) 2017, zhllxt
 *
 * Author   : zhllxt
 * QQ       : 37792738
 * Email    : 37792738@qq.com
 * 
 */


#pragma once

#include <cstdint>
#include <cstddef>

namespace zdb2
{

/**
 * Global defines, macros and types
 *
 * @file
 */



/* --------------------------------------------- SQL standard value macros */


/**
 * Standard millisecond timeout value for a database call. 
 */
const static std::size_t DEFAULT_TIMEOUT = 3000;

/**
 * The default maximum number of database connections
 */
const static std::size_t DEFAULT_MAX_CONNECTIONS = 20;


/**
 * The initial number of database connections
 */
const static std::size_t DEFAULT_INIT_CONNECTIONS = 5;


/**
 * The standard sweep interval in seconds for a ConnectionPool reaper thread
 */
const static std::size_t DEFAULT_SWEEP_INTERVAL = 60;


/**
 * Default Connection timeout in seconds, used by reaper to remove
 * inactive connections
 */
const static std::size_t DEFAULT_CONNECTION_TIMEOUT = 30;


/**
 * Default TCP/IP Connection timeout in seconds, used when connecting to
 * a database server over a TCP/IP connection
 */
const static std::size_t DEFAULT_TCP_TIMEOUT = 3;


/**
 * MySQL default server port number
 */
const static unsigned short MYSQL_DEFAULT_PORT = 3306;


/**
 * PostgreSQL default server port number
 */
const static unsigned short POSTGRESQL_DEFAULT_PORT = 5432;


/**
 * Oracle default server port number
 */
const static unsigned short ORACLE_DEFAULT_PORT = 1521;


/**
 * SQLServer default server port number
 */
const static unsigned short SQLSERVER_DEFAULT_PORT = 1433;

}
