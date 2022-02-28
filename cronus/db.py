from sqlalchemy import (
    create_engine, Table, Column, Integer, 
    Boolean, Date, DateTime, Float,
    Text, Time, String, MetaData)

from sqlalchemy.engine import reflection
from sqlalchemy.orm import sessionmaker


DATA_TYPES = {
    "str"   : String,
    "text"  : Text,
    "bool"  : Boolean,
    "int"   : Integer,
    "float" : Float,
    "date"  : Date,
    "datetime" : DateTime
}

class DB:

    def __init__(self, db, echo=False):
        self.engine = create_engine(db, echo=echo, connect_args={'check_same_thread': False})
        self.meta   = MetaData()
        self._connection = None
        self.inspector   = reflection.Inspector.from_engine(self.engine)
        self.load_tables()

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.engine.connect()
        
        return self._connection

    @property
    def tables(self):
        return self.meta.tables

    def table(self, name):
        return _Table(name, self)

    def create_table(self, name, fields):
        create = False

        if name not in self.tables:        
            columns = [
                self._column_from_dict(field) for field in fields
            ]
            table = Table(name, self.meta, *columns)

            self.meta.create_all(self.engine)

            return table
    
    def load_tables(self):
        for table in self.inspector.get_table_names():
            columns = self.get_columns(table)
            columns = [self._column_from_dict(column) for column in columns ]

            Table(table, self.meta, *columns)
    
    def get_columns(self, table):
        return self.inspector.get_columns(table)
    
    def drop_table(self, name):
        table = self.tables.get(name)

        if table is not None:
            table.drop(self.engine)

    def _column_from_dict(self, field):
        name   = field.get("name")
        ftype  = field.get("type")

        column = Column(name, ftype)

        if field.get("primary_key"):
            column.primary_key = field.get("primary_key")
        
        if field.get("unique"):
            column.unique = field.get("unique")
        
        if field.get("nullable"):
            column.nullable = field.get("nullable")
        
        if field.get("default"):
            column.default = field.get("default")

        return column

class QueryBuilder:

    def __init__(self):
        self.stmt = None
    
    def select(self):
        '''
        column = self.columns.get(column)

        if column is not None:
            self.stmt = self.table.select(column)
        else:
        '''
        self.stmt = self.table.select()
        return self

    def update(self):
        self.stmt = self.table.update()
        return self

    def delete(self):
        self.stmt = self.table.delete()
        return self
    
    def values(self, *args, **kwargs):
        self.stmt = self.stmt.values(*args, **kwargs)
        return self

    def where(self, *filters):
        
        for column, op, value in filters:
            column = self.columns.get(column)
            if op == "==":
                self.stmt = self.stmt.where(column == value)
            elif op == "!=":
                self.stmt = self.stmt.where(column != value)
            elif op == ">":
                self.stmt = self.stmt.where(column > value)
            elif op == ">=":
                self.stmt = self.stmt.where(column >= value)
            elif op == "<":
                self.stmt = self.stmt.where(column < value)
            elif op == "<=":
                self.stmt = self.stmt.where(column <= value)
            
        return self
    
    def execute(self):
        return self.db.session.execute(self.stmt)


class _Table(QueryBuilder):

    def __init__(self, name, db):
        self.db      = db
        self.name    = name
        self.table   = db.meta.tables[name]

        super().__init__()

    @property
    def columns(self):
        return { c.name : c for c in self.table.columns }

    def get_column(self, name):
        return self.columns.get(name)

    def insert(self, *args, **kwargs):
        ins = self.table.insert()
        ins = ins.values(**kwargs)

        self.db.session.execute(ins)
    
    def insertmany(self, *args):
        ins = self.table.insert()
        ins = ins.values(*args)
        
        self.db.session.execute(ins)
    
    def add_column(self, field):
        column = self.db._column_from_dict(field)
        column_name = column.compile(dialect=self.db.engine.dialect)
        
        if self.get_column(column_name) is not None:
            column_type = column.type.compile(self.db.engine.dialect)
        
            self.db.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % (self.name, column_name, column_type))

            return column
