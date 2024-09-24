import prestodb


connection_info = {
    'host': 'ec2-98-80-246-43.compute-1.amazonaws.com',
    'port': 8989,
    'user': 'python-user',
    'catalog': 'analytics',
    'schema': 'tcga_brca'
}

def query(sql_str):
    """Issue a query to the prestodb instance. This function wraps building the
    connection and issuing the query. To change destination endpoint, mutate the
    connection_info dict in this namespace (temporary)."""
    with prestodb.dbapi.connect(**connection_info) as conn:
       cur = conn.cursor()
       cur.execute(sql_str)
       return cur.fetchall()
