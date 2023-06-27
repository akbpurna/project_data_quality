# # import threading
# # import time


# # def some_process(thread_num):
# #     count = 0
# #     while count < 5:
# #         time.sleep(0.5)
# #         count += 1
# #         print(thread_num, time.ctime(time.time()))
# #         print('number of alive threads:{}'.format(threading.active_count()))


# # def create_thread():
# #     try:
# #         for i in range(1, 100):  # trying to spawn 555 threads.
# #             thread = threading.Thread(target=some_process, args=(i,))
# #             thread.start()

# #             if threading.active_count() == 20:  # set maximum threads.
# #                 thread.join()

# #             print(threading.active_count())  # number of alive threads.

# #     except Exception as e:
# #         print("Error: unable to start thread {}".format(e))


# # if __name__ == '__main__':
# #     create_thread()

# # SuperFastPython.com
# # report the default number of worker threads on your system
# from concurrent.futures import ThreadPoolExecutor
# # create a thread pool with the default number of worker threads
# executor = ThreadPoolExecutor()
# # report the number of worker threads chosen by default
# print(executor._max_workers)


# # report the number of CPUs in your system visible to Python
# import os
# print(os.cpu_count())

cursor.close()
connection.close()