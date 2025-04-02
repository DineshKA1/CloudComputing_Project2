import psycopg2

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
    db_config = {
        "dbname": "tpch",
        "user": "postgres",
        "password": "password",
        "host": "localhost",
        "port": "5432"
    }
    
    sample_query = "SELECT * FROM customer LIMIT 10;"
    print(get_query_plan(sample_query, db_config))
