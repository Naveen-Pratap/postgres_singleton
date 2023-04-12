"""
This is a thread safe singleton wrapper for Postgres DB Threaded connection pool.
"""
import psycopg2
from psycopg2 import pool
from threading import Lock


class SingletonMeta(type):
    """Singleton metaclass. Set the metaclass for any object as this class to
    turn it into a singleton class.
    """

    _instances = {}

    # use this lock to keep the singleton thread safe
    _lock = Lock()

    def __call__(cls, *args, **kwargs):
        """This gets called for every new initialisation of child class.

        Returns: Child class object.
        """
        with cls._lock:
            # First time initialisation
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance

            ret = cls._instances[cls]
            print(f"using object {ret} with id: <{id(ret)}>")
            return ret


class PostgresDB(metaclass=SingletonMeta):
    """Singleton class for PostgresDB.

    You can add more methods like a run_query according to your requirements. For now
    I will keep it bare minimum, and rely on the pool attribute to provide functionality.
    """

    def __init__(self, minConnection=5, maxConnection=20, *args, **kwargs):
        self.pool = pool.ThreadedConnectionPool(
            minConnection, maxConnection, *args, **kwargs
        )


class BookRepository:
    """A simple class using the repository pattern to show utility of the singleton DB object."""

    def __init__(self):
        self.db = PostgresDB()

    def get_book_by_name(self, name):
        conn = self.db.pool.getconn()

        query = "select * from books where name=%s"
        args = (name,)
        result = None

        with conn.cursor() as cursor:
            cursor.execute(query, args)
            result = cursor.fetchall()

        self.db.pool.putconn(conn)

        return result


if __name__ == "__main__":
    # first initialisation
    # any subsequent calls to this will return the same object
    PostgresDB()

    books = BookRepository()
    out = books.get_book_by_name("The Stranger")

    print(out)
