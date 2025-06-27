import importlib
import threading
import time
import yaml
from queue import Queue  # ✅ use this instead



class YamlPipelineExecutor(threading.Thread):
    def __init__(self, pipeline_location):
        super(YamlPipelineExecutor, self).__init__()
        self._pipeline_location = pipeline_location
        self._queues = {}
        self._workers = {}
        self._queue_consumers = {}
        self._downstream_queues = {}
        
    def _load_pipeline(self):
        with open(self._pipeline_location, 'r') as file:
            self._yaml_data = yaml.safe_load(file)
        
    def _initialize_queues(self):
        for queue in self._yaml_data['queues']:
            queue_name = queue['name']
            self._queues[queue_name] = Queue()
            
    def _initialize_workers(self):
        for worker in self._yaml_data['workers']: #import
            WorkerClass = getattr(importlib.import_module(worker['location']), worker['class'])
            
            input_queue = worker.get('input_queue')
            output_queues = worker.get('output_queues', [])
            worker_name = worker['name']
            num_instances = worker.get('instances', 1)
            
            self._downstream_queues[worker_name] = output_queues
            if input_queue is not None:
                self._queue_consumers[input_queue] = num_instances
                
            init_params = {
                'input_queue': self._queues[input_queue] if input_queue else None,
                'output_queue': self._queues[output_queues[0]] if output_queues else None
            }
            
            input_values = worker.get('input_values')
            if input_values is not None:
                init_params['input_values'] = input_values
                
            self._workers[worker_name] = []
            ## WorkerClass(input_queue=self._queues['SymbolQueue'], output_queue=[self._queues['PostgresUploading']])
            for i in range(num_instances):
                self._workers[worker_name].append(WorkerClass(**init_params))
    
    def _join_workers(self):
        for worker_name in self._workers:
            for worker_thread in self._workers[worker_name]:
                worker_thread.join()
                
                
    def process_pipeline(self):
        self._load_pipeline()
        self._initialize_queues()
        self._initialize_workers()
        # self._join_workers()

 
    def run(self):
        self.process_pipeline()
        total_workers_alive = 0
        while True:
            total_workers_alive = 0
            worker_stats = []
            to_del = []
            for worker_name in self._workers:
                total_worker_threads_alive = 0
                for worker_thread in self._workers[worker_name]:
                    if worker_thread.is_alive():
                        total_worker_threads_alive += 1
                if total_worker_threads_alive == 0:
                    if self._downstream_queues[worker_name] is not None and isinstance(self._downstream_queues[worker_name], list):
                        for output_queue in self._downstream_queues[worker_name]:
                            number_of_consumers = self._queue_consumers[output_queue]
                            for _ in range(number_of_consumers):
                                self._queues[output_queue].put("DONE")
                                
                    # del self._workers[worker_name]
                    
                    to_del.append(worker_name)
                
                worker_stats.append([worker_name, total_worker_threads_alive])
            print(worker_stats)
                
            if total_workers_alive == 0:
                break
            
            
            # queue_stats = []
            # for queue in self._queues:
            #     queue_stats.append([queue, self._queues[queue].qsize()])
                
            # print(queue_stats)
            
            for worker_name in to_del:
                del self._workers[worker_name]
            time.sleep(1)  # Keep the thread alive, or implement a proper shutdown mechanism
            