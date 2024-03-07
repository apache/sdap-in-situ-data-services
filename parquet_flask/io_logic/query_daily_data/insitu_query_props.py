class InsituQueryProps:
    def __init__(self):
        self.__provider = None
        self.__project = None
        self.__timestamp = None
        self.__size = 1000
        self.__min_lat_lon = []
        self.__max_lat_lon = []
        self.__variable = []
        self.__columns = []
        self.__marker = []

    @property
    def min_lat_lon(self):
        return self.__min_lat_lon

    @min_lat_lon.setter
    def min_lat_lon(self, val):
        """
        :param val:
        :return: None
        """
        self.__min_lat_lon = val
        return

    @property
    def max_lat_lon(self):
        return self.__max_lat_lon

    @max_lat_lon.setter
    def max_lat_lon(self, val):
        """
        :param val:
        :return: None
        """
        self.__max_lat_lon = val
        return

    @property
    def provider(self):
        return self.__provider

    @provider.setter
    def provider(self, val):
        """
        :param val:
        :return: None
        """
        self.__provider = val
        return

    @property
    def project(self):
        return self.__project

    @project.setter
    def project(self, val):
        """
        :param val:
        :return: None
        """
        self.__project = val
        return

    @property
    def timestamp(self):
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, val):
        """
        :param val:
        :return: None
        """
        self.__timestamp = val
        return

    @property
    def size(self):
        return self.__size

    @size.setter
    def size(self, val):
        """
        :param val:
        :return: None
        """
        self.__size = val
        return

    @property
    def variable(self):
        return self.__variable

    @variable.setter
    def variable(self, val):
        """
        :param val:
        :return: None
        """
        self.__variable = val
        return

    @property
    def columns(self):
        return self.__columns

    @columns.setter
    def columns(self, val):
        """
        :param val:
        :return: None
        """
        self.__columns = val
        return

    @property
    def marker(self):
        return self.__marker

    @marker.setter
    def marker(self, val):
        """
        :param val:
        :return: None
        """
        self.__marker = val
        return
