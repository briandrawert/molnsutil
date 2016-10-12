class Log:
    def __init__(self, verbose=True):
        self.verbose = verbose

    def write_log(self, message):
        if self.verbose:
            print message
