import redis
import time
import os
import signal
import logging
import sys
import json
import multiprocessing
import optparse
from geventconnpool import ConnectionPool
from gevent import socket, ssl

class HardWorker:
  def perform(self, word):
    print("hello")
    sys.stdout.flush()
    print(word)


class RedisPool(ConnectionPool):
  def _new_connection(self):
    conn = redis.from_url(os.environ['REDIS_URL'])
    return conn

class Poller:
  def __init__(self, logger):
    self.logger = logger

  def pool(self, conn):
    with conn.get() as client:
      job = client.rpop("sidekiq:queue:python")
    WorkerSupervisor(self.logger).new_job(job)
    time.sleep(0.5)
    self.pool(conn)

class WorkerSupervisor:
  def __init__(self, logger):
    self.logger = logger

  def new_job(self, job):
    if job:
      self.logger.info('Start new worker')
      try:
        payload = json.loads(job)
        klass = payload['class']
        globals()[klass]().perform(*payload['args'])
      except (RuntimeError, TypeError, NameError, ValueError):
        self.logger.warning('Failed to enqueue worker.', exc_info=True)
        pass
      self.logger.info('Done worker')

class WorkerEngine:
  def __init__(self, pool, name, concurrency):
    self.pool = pool
    self.name = name
    self.concurrency = concurrency
    self.threads = []

  def start(self, logger):
    while (self.concurrency > len(self.threads)):
      process = multiprocessing.Process(name=self.name, target=Poller(logger).pool, args=(self.pool,))
      process.start()
      self.threads.append(process)
      self.concurrency = self.concurrency - 1
    return self.threads


def main():
  multiprocessing.log_to_stderr()
  logger = multiprocessing.get_logger()
  logger.setLevel(logging.INFO)
  pool = RedisPool(30)
  p = WorkerEngine(pool, 'Fetcher', 25)
  p.start(logger)

if (__name__ == '__main__'):
  main()
