class ThreadHelper():
    def __init__(self, nr_threads):
        self.count = 0
        self.duration = 0
        self.capacity_units = 0
        self.threads = []
        self.nr_threads = nr_threads

    def add(self, thread):
        self.threads.append(thread)

    def try_start(self):
        if len(self.threads) == self.nr_threads:
            for thread in self.threads:
                thread.start()

            for thread in self.threads:
                thread.join()
                self.duration = self.duration + thread.response['Duration']
                self.count = self.count + thread.count
                self.capacity_units = self.capacity_units + thread.capacity_units

            self.threads = []
