import Queue
import logging
import multiprocessing
import random
import time

FORMAT = '%(asctime)s [%(levelname)s]: %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)


class Packet(object):
    def __init__(self, msg, size):
        self.size = size
        self.msg = msg


class LeakyBucket:
    def __init__(self, max_rate=1024):
        self.buffer = multiprocessing.Queue()
        self.max_rate = max_rate
        self.remainder = max_rate
        self.next_packet = None

    def push(self, packet):
        self.buffer.put(packet)

    def run(self):
        p = multiprocessing.Process(target=self.pop)
        p.start()

    def send_next_packet(self):
        self.remainder -= self.next_packet.size
        if self.remainder < 0:
            return False

        logger.info("Send '{msg}' with size '{size}'. Remain: {remain}".format(msg=self.next_packet.msg,
                                                                               size=self.next_packet.size,
                                                                               remain=self.remainder))
        self.next_packet = None
        return True

    def pop(self):
        while True:
            logger.info("--- Send packages round ---")
            while self.remainder > 0:
                try:
                    if self.next_packet:
                        result = self.send_next_packet()
                        if not result:
                            break
                    self.next_packet = self.buffer.get(block=False, timeout=1)
                except Queue.Empty:
                    logger.info("Buffer empty")
                    break
            self.remainder = self.max_rate
            time.sleep(1)


class LeakyBucketProducer:
    def __init__(self, lb, name):
        self.lb = lb
        self.name = name
        self.packet_counter = 0

    def run(self):
        p = multiprocessing.Process(target=self.send)
        p.start()

    def send(self):
        while True:
            self.lb.push(
                Packet("Packet %s-%s" % (self.name, self.packet_counter), random.choice((64, 128, 256, 512, 1024))))
            self.packet_counter += 1
            time.sleep(random.random())


if __name__ == '__main__':
    lb = LeakyBucket()
    lb.run()

    for i in range(10):
        lbp = LeakyBucketProducer(lb, str(i))
        lbp.run()
