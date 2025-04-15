import psycopg2
from pipesyntax import convert_to_pipe_syntax

class Database:
    def __init__(self, dbname, user, password, host, port = 5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port   
        )
    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(self, query):
        if not self.conn:
            raise ValueError("Not connected to the database")
        cur = self.conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        cur.close()
        return result
    def get_plan_json(self, sql):
        if not self.conn:
            raise ValueError("Not connected to the database")
        cur = self.conn.cursor()
        cur.execute("EXPLAIN (FORMAT JSON, ANALYZE FALSE, COSTS TRUE) " + sql)
        plan = cur.fetchone()[0]    
        cur.close()
        return plan

    # current use this one
    def get_plan_original(self, sql): 
        if not self.conn:
            raise ValueError("Not connected to the database")
        cur = self.conn.cursor()
        cur.execute(f"EXPLAIN ANALYZE {sql}")
        plan = "\n".join(row[0] for row in cur.fetchall())
        cur.close()
        return plan

#test usage
if __name__ == "__main__":
    Database = Database("tpch", "postgres", "pwd", "localhost", "5432")
    with open('example/query2.txt', 'r') as file:
        query = file.read().strip()
    Database.connect()
    plan = Database.get_plan_original(query)
    # plan = Database.get_plan_json(query)
    # print(plan_json)
    print(plan)
    print("----")
    print(convert_to_pipe_syntax(plan))
    Database.disconnect() 
