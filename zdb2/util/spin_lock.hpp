/*
 * COPYRIGHT (C) 2017, zhllxt
 *
 * Author   : zhllxt
 * QQ       : 37792738
 * Email    : 37792738@qq.com
 * referenced from boost/smart_ptr/detail/spinlock_std_atomic.hpp
 *
 */


#pragma once

#include <atomic>
#include <thread>

namespace zdb2
{

	class spin_lock
	{
	public:

		bool try_lock()
		{
			return !v_.test_and_set(std::memory_order_acquire);
		}

		void lock()
		{
			for (unsigned k = 0; !try_lock(); ++k)
			{
				if (k < 16)
				{
					std::this_thread::yield();
				}
				else if (k < 32)
				{
					std::this_thread::sleep_for(std::chrono::milliseconds(0));
				}
				else
				{
					std::this_thread::sleep_for(std::chrono::milliseconds(1));
				}
			}
		}

		void unlock()
		{
			v_.clear(std::memory_order_release);
		}

	public:
		std::atomic_flag v_ = ATOMIC_FLAG_INIT;

	};

}

