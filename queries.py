BASE = "http://example.org/"
GRAPH = "http://example.org/benchmark"

QUERIES = {
    "select_simple": {
        "operation": "SELECT",
        "sparql": "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 100",
    },
    "select_filter": {
        "operation": "SELECT",
        "sparql": f"""
            SELECT ?s ?value WHERE {{
                ?s <{BASE}value> ?value .
                FILTER(?value > 500)
            }} LIMIT 100
        """,
    },
    "select_optional": {
        "operation": "SELECT",
        "sparql": f"""
            SELECT ?s ?label WHERE {{
                ?s a <{BASE}Entity> .
                OPTIONAL {{ ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label }}
            }} LIMIT 100
        """,
    },
    "ask": {
        "operation": "ASK",
        "sparql": f"ASK {{ <{BASE}entity/1> ?p ?o }}",
    },
    "construct": {
        "operation": "CONSTRUCT",
        "sparql": f"""
            CONSTRUCT {{ ?s ?p ?o }}
            WHERE {{ ?s a <{BASE}Entity> ; ?p ?o }}
            LIMIT 100
        """,
    },
}

UPDATES = {
    "insert_data": {
        "operation": "INSERT",
        "sparql": f"""
            INSERT DATA {{
                GRAPH <{GRAPH}> {{
                    {" ".join(
                        f"<{BASE}benchmark/entity/{i}> <{BASE}benchmarkValue> {i} ."
                        for i in range(100)
                    )}
                }}
            }}
        """,
    },
    "delete_data": {
        "operation": "DELETE",
        "sparql": f"""
            DELETE DATA {{
                GRAPH <{GRAPH}> {{
                    {" ".join(
                        f"<{BASE}benchmark/entity/{i}> <{BASE}benchmarkValue> {i} ."
                        for i in range(100)
                    )}
                }}
            }}
        """,
    },
    "update": {
        "operation": "UPDATE",
        "sparql": f"""
            WITH <{GRAPH}>
            DELETE {{ ?s <{BASE}benchmarkValue> ?old }}
            INSERT {{ ?s <{BASE}benchmarkValue> ?new }}
            WHERE {{
                ?s <{BASE}benchmarkValue> ?old .
                BIND(?old + 1 AS ?new)
            }}
        """,
    },
}
