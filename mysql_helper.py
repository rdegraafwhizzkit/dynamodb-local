def resultset_iterator(cursor, fetch_size=1000):
    while True:
        results = cursor.fetchmany(fetch_size)
        if not results:
            break
        for result in results:
            yield result
