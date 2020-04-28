import collections


class Arg:
    """
    Данные запроса.
    """

    def __init__(self, command_line):
        """
        Конструктор.
        :param command_line: строка запроса.
        """
        if command_line is None:
            raise ValueError(f'no argument specified: \'{command_line}\'')
        self.__command_line = command_line.split(' ', maxsplit=2)

    @property
    def command(self):
        """
        Команда.
        :return: команда запроса.
        """
        if not self.__command_line[0] or self.__command_line[0] == '':
            raise ValueError(f'no command specified: \'{self.__command_line[0].upper()}\'')
        return self.__command_line[0].upper()

    @property
    def name(self):
        """
        Наименование данного.
        :return: наименование данного запроса.
        """
        if len(self.__command_line) < 2:
            raise ValueError(f'no argument name specified: \'{self.command}\'')
        return self.__command_line[1]

    @property
    def value(self):
        """
        Значение данного
        :return: значение данного запроса.
        """
        if len(self.__command_line) < 3:
            raise ValueError(f'no argument value specified: \'{self.command}\'')
        return int(self.__command_line[2])


class PizDB:
    """
    Основной класс базы данных.
    """
    __version = 'Pizza db. Version 1.0. Small database.'

    def __init__(self):
        """
        Конструктор класса базы данных.
        """
        self.__performed = True
        self.__commands = ['SET', 'GET', 'UNSET', 'COUNTS', 'END', 'BEGIN', 'ROLLBACK', 'COMMIT']
        self.__storage = {}

    def __end(self):
        """
        Завершить работу программы.
        """
        self.__performed = False

    def __set(self, data):
        """
        Сохранить объект хранения.
        :param data: параметры запроса.
        """
        self.__storage[data.name] = data.value

    def __get(self, data):
        """
        Получить объект хранения.
        :param data: параметры запроса.
        :return: объект хранения.
        """
        return self.__storage[data.name] if data.name in self.__storage else 'NULL'

    def __unset(self, data):
        """
        Удалить объект хранения.
        :param data: параметры запроса.
        """
        del self.__storage[data.name]

    def __counts(self, data):
        """
        Получить количество хранимых данных соответствующих параметрам запроса.
        :param data: параметры запроса.
        :return: количество хранимых данных соответствующих параметрам запроса.
        """
        values = collections.Counter(self.__storage.values())
        return values[int(data.name)] if int(data.name) in values.elements() else 0

    def __execute(self, data):
        """
        Выполнить запрос.
        :param data: параметры запроса.
        :return: результат выполнения запроса.
        """
        if data.command not in self.__commands:
            raise NameError(f'entered command does not exist: \'{data.command}\'')
        if data.command == 'SET':
            self.__set(data)
        elif data.command == 'GET':
            return self.__get(data)
        elif data.command == 'UNSET':
            self.__unset(data)
        elif data.command == 'COUNTS':
            return self.__counts(data)
        elif data.command == 'END':
            self.__end()
        elif data.command == 'BEGIN':
            return self.__begin()
        elif data.command == 'ROLLBACK':
            return self.__rollback()
        elif data.command == 'COMMIT':
            return self.__commit()

    def execute(self, data):
        """
        Выполнить запрос.
        :param data: параметры запроса.
        :return: результат выполнения запроса.
        """
        return self.__execute(Arg(data))

    @property
    def performed(self):
        """
        Флаг завершения работы программы.
        :return: True - программа продолжает работат, False - завершить работу программы.
        """
        return self.__performed

    @property
    def version(self):
        """
        Версия программы.
        :return: версия программы.
        """
        return self.__version


def run():
    """
    Точка входа.
    """
    db = PizDB()
    print(db.version)
    while True:
        try:
            result = db.execute(input())
            if not db.performed:
                break
            if result is not None:
                print(result)

        except NameError as error:
            print(error)
        except ValueError as error:
            print(error)


run()
