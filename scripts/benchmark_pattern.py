import redis
import sys
import time
import random

from redisgraph import Node, Edge, Graph, Path

import traceback

N = 1
WARMUP_N = 0

# Connect to the server
rg_conn = redis.Redis(host='localhost', port=6379)

def run_benchmark(graph, query):
  redis_graph = Graph(graph, rg_conn)

  # Loop for each query
  total_query_time = 0
  total_time = 0
  total_n = 0

  # Query
  result = exec_query(redis_graph, query)

  # Calculate time consumed
  total_query_time += result['query_time']
  total_time += result['time']

  print(f'{graph},{total_query_time},{total_time}')

def exec_query(redis_graph, query):
  start = time.time() * 1000
  result = redis_graph.query(query, timeout=1000000)
  end = time.time() * 1000

  return {
    "query_time": result.run_time_ms,
    "time": end - start
  }

if len(sys.argv) != 3:
	print("Error: wrong command usage!")
	print("Command usage: python benchmark.py [graph_name] [query]")
	sys.exit(0)

run_benchmark(sys.argv[1], sys.argv[2])
