import importlib

import yaml
from queue import Queue  # ✅ use this instead



class YamlPipelineExecutor():
    def __init__(self, pipeline_location):
        self._pipeline_location = pipeline_location
        self._queues = {}
        self._workers = {}
        
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
        self._join_workers()

 