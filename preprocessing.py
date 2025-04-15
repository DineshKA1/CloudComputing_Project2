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

def get_json_plan(conn, sql: str):
    cur = conn.cursor()
    cur.execute("EXPLAIN (FORMAT JSON, ANALYZE FALSE, COSTS TRUE) " + sql)
    plan = cur.fetchone()[0]      # psycopg2 已经把 JSON 解析成 Python 对象
    cur.close()
    return plan

def get_query_plan(sql_query, db_config):
    """
    connect to PostgreSQL and retrieve the execution plan for the given query
    :param sql_query: SQL query to analyze
    :param db_config: dictionary containing database connection parameters
    :return: query execution plan as a string
    """
    try:
        #connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        
        cur = conn.cursor()
        
        #EXPLAIN ANALYZE to get the query execution plan
        cur.execute(f"EXPLAIN ANALYZE {sql_query}")
        plan = "\n".join(row[0] for row in cur.fetchall())
        #close connection
        cur.close()
        conn.close()
        
        return plan
    
    except Exception as e:
        return f"Error retrieving query plan: {str(e)}"

#test usage
if __name__ == "__main__":
    Database = Database("tpch", "postgres", "Afoafo2025!!", "localhost", "5432")
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
