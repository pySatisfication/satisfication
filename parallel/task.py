#!/usr/bin/python
import sys

class InutDataTask(object):
    def __init__(self, task_type='off_file', param=None):
        self._type = task_type
        self.parameters = param
    
    def procedure(self):
        raise NotImplementedError("Input data reading method not implemented")

class OfflineDataTask(InputDataTask):
    def __init__(self, task_type='off_file'):
        super(InutDataTask, self).__init__(task_type, param)

    def procedure(self):
        file_path = []

        pass

    
    

