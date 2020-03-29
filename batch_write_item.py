import threading


class BatchWriteItemThread(threading.Thread):

    def __init__(self, client, table_name: str, items: list):
        threading.Thread.__init__(self)

        self.response = {}
        self.count = len(items)
        self.capacity_units = 0
        self.duration = 0

        self.client = client
        self.table_name = table_name
        self.items = items

    def run(self):
        self.response = self.client.batch_write_item(
            Fields=['Duration', 'ConsumedCapacity'],
            RequestItems={self.table_name: self.items},
            ReturnConsumedCapacity='TOTAL'
        )
        self.capacity_units = self.response['ConsumedCapacity'][0]['CapacityUnits']
        self.duration = self.response['Duration']


class BatchWriteItemThreadHelper:
    def __init__(self, nr_threads):
        self.count = 0
        self.duration = 0
        self.capacity_units = 0
        self.threads = []
        self.nr_threads = nr_threads

    def add(self, thread: BatchWriteItemThread):
        self.threads.append(thread)

    def try_start(self):
        if len(self.threads) == self.nr_threads:
            self.start()
            self.threads = []

    def start(self):
        for thread in self.threads:
            thread.start()

        for thread in self.threads:
            thread.join()
            self.duration = self.duration + thread.duration
            self.count = self.count + thread.count
            self.capacity_units = self.capacity_units + thread.capacity_units
