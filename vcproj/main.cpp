// compile application on linux system can use below command :
// g++ -std=c++11 -lpthread -lrt -ldl main.cpp -o main.exe -I /usr/local/include -I ../../zdb2 -L /usr/local/lib -l sqlite3 

#include <clocale>
#include <climits>
#include <csignal>
#include <locale>
#include <limits>
#include <thread>
#include <chrono>

//#define SQLITEUNLOCK

#include <zdb2/zdb.hpp>



int main(int argc, char *argv[])
{
#if defined(WIN32) || defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS_)
	// Detected memory leaks on windows system,linux has't these function
	_CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);

	// the number is the memory leak line num of the vs output window content.
	//_CrtSetBreakAlloc(6540);
#endif

#if defined(__unix__) || defined(__linux__)
	// set linux lang environment to gbk,if you don't set linux lang to gbk,because linux default lang
	// is utf-8 and this source file format is gbk,so when you call wcstombs,you will can't getting the
	// desired results and may be cause crash.
	// when we set gbk,it will can't printf chinese character in linux system.you need call string to 
	// utf8 function to convert the buffer to utf-8 so it can printf correct,or you can change the linux
	// terminal language environment to zh_CN.gbk.
	const char * locale = (argc > 1 ? argv[1] : "zh_CN.gbk");
#elif defined(WIN32) || defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS_)
	// set defaul lang environment,windows default lang is gbk
	const char * locale = (argc > 1 ? argv[1] : "chs");
#endif
	if (std::setlocale(LC_ALL, locale) == nullptr)
	{
		std::printf("Fatal Error : set locale '%s' failed,application will be exit...\n", locale);
		std::this_thread::sleep_for(std::chrono::seconds(5));
		return -1;
	}

	std::shared_ptr<zdb2::url> url_ptr = std::make_shared<zdb2::url>("sqlite://engine.db3?synchronous=normal&heap_limit=8000&foreign_keys=on");

	std::shared_ptr<zdb2::pool> pool_ptr = std::make_shared<zdb2::pool>(url_ptr);

	std::shared_ptr<zdb2::connection> conn = pool_ptr->get();

	conn->execute("update tbl_global_config set beat_port=%u", 1111);

	auto result = conn->query("select * from tbl_global_config");
	if (result->next_row())
	{
		auto beat_port = result->get_string("beat_port");
		std::printf("beat_port = %s loca_port=%d loca_mode=%s\n", beat_port, result->get_int("loca_port"), result->get_string("loca_mode"));
	}

#if defined(WIN32) || defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS_)
	system("pause");
#endif

	return 0;
};

