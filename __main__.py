from collections import Counter, deque
from enum import Enum
from datetime import datetime


class TypeCommand(Enum):
    BEGIN = 0
    SET = 1
    GET = 2
    UNSET = 3
    COUNTS = 4
    ROLLBACK = 5
    COMMIT = 6


class Arg:
    def __init__(self, command_line):
        if command_line is None:
            raise ValueError(f'unknown command')
        self.__command_line = command_line.split(' ', maxsplit=2)

    @property
    def command(self):
        return self.__command_line[0].upper()

    @property
    def name(self):
        return self.__command_line[1] if len(self.__command_line) > 1 else None

    @property
    def value(self):
        return int(self.__command_line[2]) if len(self.__command_line) > 2 else None


class PizDB:
    def __init__(self):
        self.__performed = True
        self.__commands = ['SET', 'GET', 'UNSET', 'COUNTS', 'END', 'BEGIN', 'ROLLBACK', 'COMMIT']
        self.__storage = {}
        self.__transaction_status = False
        self.__transaction_log_archive = deque()
        self.__transaction_buf = {}
        self.__transaction_deleted = {}
        self.__transaction_log = deque()

    def __get_transaction_value(self, name):
        result = None
        if self.__transaction_status:
            result = self.__transaction_buf[name] if name in self.__transaction_buf else None
        if result is None:
            result = self.__storage[name] if name in self.__storage else None
        return result

    def __to_archive(self, log):
        while True:
            try:
                log_info = log.popleft()
                self.__transaction_log_archive.append(log_info)
            except IndexError:
                break

    def __end(self):
        self.__performed = False

    def __set(self, data):
        if self.__transaction_status:
            value = self.__get_transaction_value(data.name)

            if data.name in self.__transaction_deleted:
                del self.__transaction_deleted[data.name]

            self.__transaction_buf[data.name] = data.value

            self.__transaction_log.append({
                'id': len(self.__transaction_log),
                'type': TypeCommand.SET,
                'time': datetime.now(),
                'name': data.name,
                'old_value': value,
                'value': data.value
            })
        else:
            self.__storage[data.name] = data.value

    def __get(self, data):
        if self.__transaction_status:
            value = self.__get_transaction_value(data.name)

            self.__transaction_log.append({
                'id': len(self.__transaction_log),
                'type': TypeCommand.GET,
                'time': datetime.now(),
                'name': data.name,
                'value': value
            })

            return value if value is not None else 'NULL'

        return self.__storage[data.name] if data.name in self.__storage else 'NULL'

    def __unset(self, data):
        if self.__transaction_status:
            value = self.__get_transaction_value(data.name)

            if value is not None:
                if data.name in self.__transaction_buf:
                    del self.__transaction_buf[data.name]
                self.__transaction_deleted[data.name] = value

            self.__transaction_log.append({
                'id': len(self.__transaction_log),
                'type': TypeCommand.UNSET,
                'time': datetime.now(),
                'name': data.name,
                'value': value
            })
        else:
            if data.name in self.__storage:
                del self.__storage[data.name]

    def __counts(self, data):
        if self.__transaction_status:
            by_storage = set([key for (key, value) in self.__storage.items() if value == int(data.name)])
            by_buf = set([key for (key, value) in self.__transaction_buf.items() if value == int(data.name)])
            by_deleted = set([key for (key, value) in self.__transaction_deleted.items() if value == int(data.name)])

            self.__transaction_log.append({
                'id': len(self.__transaction_log),
                'type': TypeCommand.COUNTS,
                'time': datetime.now()
            })

            return len(by_storage.union(by_buf).difference(by_deleted))
        else:
            values = Counter(self.__storage.values())
            return values[int(data.name)] if int(data.name) in values.elements() else 0

    def __begin(self):
        self.__transaction_status = True

        self.__transaction_log.append({
            'id': 0,
            'type': TypeCommand.BEGIN,
            'time': datetime.now()
        })

    def __rollback(self):
        if self.__transaction_status:
            while True:
                transaction_info = self.__transaction_log.pop()
                if transaction_info['type'] == TypeCommand.UNSET:
                    if transaction_info['name'] in self.__transaction_deleted:
                        del self.__transaction_deleted[transaction_info['name']]
                    self.__transaction_buf[transaction_info['name']] = transaction_info['value']
                elif transaction_info['type'] == TypeCommand.SET:
                    self.__transaction_buf[transaction_info['name']] = transaction_info['old_value']
                elif transaction_info['type'] == TypeCommand.BEGIN:
                    break

            self.__transaction_log.append({
                'id': len(self.__transaction_log),
                'type': TypeCommand.ROLLBACK,
                'time': datetime.now()
            })

    def __commit(self):
        if self.__transaction_status:
            for key in self.__transaction_buf:
                if self.__transaction_buf[key] is not None:
                    self.__storage[key] = self.__transaction_buf[key]

            for key in self.__transaction_deleted:
                if key in self.__storage:
                    del self.__storage[key]

            self.__transaction_log.append({
                'id': len(self.__transaction_log),
                'type': TypeCommand.COMMIT,
                'time': datetime.now()
            })

            self.__transaction_buf.clear()
            self.__transaction_deleted.clear()
            self.__to_archive(self.__transaction_log)
            self.__transaction_log.clear()
            self.__transaction_status = False

    def __execute(self, data):
        if data.command not in self.__commands:
            raise NameError(f'unknown command')
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
            self.__begin()
        elif data.command == 'ROLLBACK':
            self.__rollback()
        elif data.command == 'COMMIT':
            self.__commit()

    def execute(self, data):
        return self.__execute(Arg(data))

    @property
    def performed(self):
        return self.__performed


def run():
    db = PizDB()
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
