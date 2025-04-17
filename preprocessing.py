import psycopg2
import pipesyntaxT

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
        cur.execute("EXPLAIN (FORMAT JSON, ANALYZE TRUE, COSTS TRUE) " + sql)
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
    Database = Database("tpch", "postgres", "Afoafo2025!!", "localhost", "5432")
    with open('example/query1.txt', 'r') as file:
        query = file.read().strip()
    Database.connect()
    # plan = Database.get_plan_original(query)
    plan_json = Database.get_plan_json(query)
    print(plan_json)
    root = pipesyntaxT.parse_qep_json(plan_json)
    result = pipesyntaxT.node_to_syntax(root)
    print("pipe-syntax result:")
    print(result)
    # print(plan)
    # print("----")
    # print(pipesyntaxT.convert_to_raw_pipe_syntax(plan))
    # root = pipesyntaxT.parse_qep(plan)
    # print(pipesyntaxT.node_to_syntax(root))
    Database.disconnect() 
