import threading


class BatchWriteItem(threading.Thread):

    def __init__(self, client, table_name, items):
        threading.Thread.__init__(self)
        self.response = {'Duration': 0}
        self.client = client
        self.table_name = table_name
        self.items = items
        self.count = len(items)

    def run(self):
        self.response = self.client.batch_write_item(
            Fields=['Duration', 'ConsumedCapacity'],
            RequestItems={self.table_name: self.items},
            ReturnConsumedCapacity='TOTAL'
        )
