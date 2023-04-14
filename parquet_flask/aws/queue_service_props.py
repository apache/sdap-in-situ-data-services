class QueueServiceProps:
    def __init__(self):
        self.__queue_url = None

    @property
    def queue_url(self):
        return self.__queue_url

    @queue_url.setter
    def queue_url(self, val):
        """
        :param val:
        :return: None
        """
        self.__queue_url = val
        return