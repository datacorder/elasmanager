import requests
import os
import json


class ElasManager():
    """
    ElasticSearch cluster management
    """

    def __init__(self):
        """
        Set cluster URI
        """
        try:
            self.host = os.environ['ES_HOST']
            self.port = os.environ['ES_PORT']

        except Exception as e:
            print('\n%s' % e)
            print('\nPlease configure required environment variables to start using ElasManager')
            exit(-1)

        try:
            self.schema = os.environ['ES_SCHEMA']
        except Exception:
            self.schema = 'http'

        self.cluster = '%s://%s:%s' % (self.schema, self.host, self.port)

    def get(self, path):
        try:
            r = requests.get('%s/%s' % (self.cluster, path))

        except Exception as e:
            raise Exception(e)

        return r.text

    def status(self):
        return self.get('_cluster/health?pretty')

    def indices(self, mask='*'):
        res = self.get('_cat/indices/%s?v&h=i' % mask).split()
        return res[1:]

    def delete(self, index):
        if index == '*' or index == '':
            raise Exception('I\'m not going to do that')

        r = requests.delete('%s/%s' % (self.cluster, index))
        return r

    def errors_monitor(self):
        res = json.loads(self.get('_cluster/allocation/explain'))
        try:
            error = res['error']['reason']
            return False

        except Exception as e:
          return res
