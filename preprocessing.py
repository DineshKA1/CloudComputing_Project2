import psycopg2
import pipesyntax

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

    def get_plan_original(self, sql): 
        if not self.conn:
            raise ValueError("Not connected to the database")
        cur = self.conn.cursor()
        cur.execute(f"EXPLAIN ANALYZE {sql}")
        plan = "\n".join(row[0] for row in cur.fetchall())
        cur.close()
        return plan

def get_query_plan(db, sql):
    return db.get_plan_json(sql)