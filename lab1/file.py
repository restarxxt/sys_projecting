from threading import Thread
from threading import Lock
import time
import hazelcast
import yaml

def parse_yaml():
	with open("hazelcast.yaml", "r") as stream:
		try:
			yaml.safe_load(stream)
		except yaml.YAMLError as exc:
			print(exc)

map_name = "my-distributed-map"

class ThreadCounter():
    # constructor
	def __init__(self):
        # initialize counter
		self._counter = 0
        # initialize lock
		self._lock = Lock()
 
    # increment the counter
	def increment(self):
		self._counter += 1


def third_task(counter):
    hz = hazelcast.HazelcastClient()
    key = "Task3"
    map = hz.get_map("my-distributed-map").blocking()
    for i in range(10000):
    	with counter._lock:
        	counter.increment()
    map.put(key, counter._counter)

def forth_task(counter):
    hz = hazelcast.HazelcastClient()
    key = "Task4"
    map = hz.get_map("my-distributed-map").blocking()
    for i in range(10000):
        counter._counter = map.get(key)
        counter._counter += 1
        map.put(key, counter._counter)

def fifth_task():
	hz = hazelcast.HazelcastClient()
	key = "Task5"
	map = hz.get_map("my-distributed-map").blocking()
	for i in range(10000):
		map.lock(key)
		try:
			value = map.get(key)
			value += 1
			map.put(key, value)
			#print(map.get(key))
		finally:
			map.unlock(key)

def sixth_task(counter):
    hz = hazelcast.HazelcastClient()
    key = "Task6"
    map = hz.get_map("my-distributed-map").blocking()
    loop = True
    for i in range(10000):
        while loop is not False:
            with counter._lock:
                oldValue = map.get(key)
                newValue = oldValue
                newValue += 1
                if (map.replace_if_same(key, oldValue, newValue)): 
                    break

def seventh_task(counter):
	hz = hazelcast.HazelcastClient()
	key = "Task7"
	map = hz.get_map("my-distributed-map").blocking()
	count = hz.cp_subsystem.get_atomic_long("counter").blocking()
	value = 0
	for i in range(10000):
		value = count.add_and_get(1)
		#print(value)
	map.put(key, value)

def output(param):
	hz = hazelcast.HazelcastClient()
	key = param
	map = hz.get_map("my-distributed-map").blocking()
	print("Finished! Result = ", map.get(key))


if __name__ == "__main__":
	parse_yaml()
	counter = ThreadCounter()

	print("=======TASK 3=======")
	start_time = time.time()
	threads = [Thread(target=third_task, args=(counter,)) for _ in range(10)]
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
	output("Task3")
	print("--- %s seconds ---" % (time.time() - start_time))

	print("\n\n=======TASK 4=======")
	hz = hazelcast.HazelcastClient()
	map = hz.get_map("my-distributed-map").blocking()
	key = "Task4"
	map.put(key, 0)
	start_time = time.time()
	threads = [Thread(target=forth_task, args=(counter,)) for _ in range(10)]
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
	output("Task4")
	print("--- %s seconds ---" % (time.time() - start_time))

	print("\n\n=======TASK 5=======")
	key = "Task5"
	map.put(key, 0)
	start_time = time.time()
	threads = [Thread(target=fifth_task) for _ in range(10)]
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
	output("Task5")
	print("--- %s seconds ---" % (time.time() - start_time))

	print("\n\n=======TASK 6=======")
	key = "Task6"
	map.put(key, 0)
	start_time = time.time()
	threads = [Thread(target=sixth_task, args=(counter,)) for _ in range(10)]
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
	output("Task6")
	#output("Task6_1")
	print("--- %s seconds ---" % (time.time() - start_time))

	print("\n\n=======TASK 7=======")
	key = "Task7"
	count = hz.cp_subsystem.get_atomic_long("counter").blocking()
	count.set(0)
	start_time = time.time()
	threads = [Thread(target=seventh_task, args=(counter,)) for _ in range(10)]
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
	output("Task7")
	print("--- %s seconds ---" % (time.time() - start_time))