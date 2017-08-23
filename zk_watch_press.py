# -*- coding:utf-8 -*-
# !/usr/bin/env python -u
"""
This module 

Authors: liuying
Date:    2017/08/23 16:58
File:    zk_watch_press.py
"""


import kazoo
import kazoo.client
import logging
import traceback
import time
from kazoo.recipe.watchers import ChildrenWatch, DataWatch


class ManyWatches(object):
    def __init__(self, zk_root_addr="127.0.0.1:2181"):
        self.zk_root_addr = zk_root_addr
        self.zk_client = None

    def zk_client_start(self):
        """
        start
        :return:
        """
        try:
            logger.debug("zk addr %s" % self.zk_root_addr)
            self.zk_client = kazoo.client.KazooClient(hosts=self.zk_root_addr)
            self.zk_client.start()
            return True
        except:
            tip = "zk_client_start failed " + traceback.format_exc();
            logger.warning(tip)
            return False

    def zk_client_stop(self):
        """
        stop
        :return:
        """
        try:
            if self.zk_client is not None:
                self.zk_client.stop()
            return True
        except:
            tip = "zk_client_stop failed " + traceback.format_exc();
            logger.warning(tip)
            return False

    def add_children_watch(self, children_watch_path, count=200):
        """
        add children watch(es)
        :param children_watch_path:
        :param count:
        :return:
        """
        try:
            if self.zk_client is None:
                logger.warning("zk client is unavailable!")
                return
            watches = []
            for i in xrange(0, count):
                watch = ChildrenWatch(self.zk_client, children_watch_path, self.children_watch_func)
                watches.append(watch)
                logger.info("add data watch %s" % (str(i),))
            return watches
        except:
            tip = "zk_client_start failed " + traceback.format_exc()
            logger.warning(tip)
            return False

    def add_data_watch(self, data_watch_path, count=200):
        """
        add data watch(es)
        :param data_watch_path:
        :param count:
        :return:
        """
        try:
            if self.zk_client is None:
                logger.warning("zk client is unavailable!")
                return
            watches = []
            for i in xrange(0, count):
                watch = DataWatch(self.zk_client, data_watch_path, self.data_watch_func)
                watches.append(watch)
                logger.info("add data watch %s" % (str(i),))
            return watches
        except:
            tip = "add_data_watch failed " + traceback.format_exc()
            logger.warning(tip)
            return

    @classmethod
    def data_watch_func(cls, data, stat):
        """
        data watch function
        :param data:
        :param stat:
        :return:
        """
        logger.info("Data is %s, Version is %s" % (data, stat.version))

    @classmethod
    def children_watch_func(cls, children):
        """
        children watch function
        :param children:
        :return:
        """
        logger.info("Children are %s" % children)

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s][%(levelname)s][%(threadName)s][%(filename)s:%(funcName)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S%F',
                        filename='/tmp/zk_watch_press.log',
                        filemode='w')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # set a format which is simpler for console use
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(threadName)s][%(filename)s:%(funcName)s:%(lineno)d] %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    # logging.getLogger('').addHandler(console)

    logger = logging.getLogger()

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-z", "--zk_address", type=str,
                        help="zookeeper root address, e.g. 127.0.0.1:2181")
    parser.add_argument("-d", "--data_watch_path", type=str, required=True,
                        help="data watch path, e.g. /clusterstate.json")
    parser.add_argument("-c", "--children_watch_path", type=str, required=True,
                        help="children watch path, e.g. /live_nodes")
    parser.add_argument("-n", "--number", type=int,
                        help="the number of each watches, e.g. 200")
    parser.add_argument("-t", "--hours", type=int,
                        help="the hours to watch, e.g. 2")
    args = parser.parse_args()
    data_watch_path = ""
    children_watch_path = ""
    count = 200
    hours = 2
    zk_address = "127.0.0.1:2181"
    if args.zk_address:
        zk_address = args.zk_address
    if args.data_watch_path:
        data_watch_path = args.data_watch_path
    if args.children_watch_path:
        children_watch_path = args.children_watch_path
    if args.number:
        count = args.number
    if args.hours:
        count = args.hours
    try:
        many_watch = ManyWatches(zk_root_addr=zk_address)
        many_watch.zk_client_start()
        print "start timestamp %s" % str(time.time())
        children_watches = many_watch.add_children_watch(children_watch_path, count)
        data_watches = many_watch.add_data_watch(data_watch_path, count)
        if children_watches and data_watches:
            time.sleep(hours * 3600)
        else:
            print "add watches failed!"
        print "end timestamp %s" % str(time.time())
        many_watch.zk_client_stop()
    except:
        tip = traceback.format_exc()
        print tip
    finally:
        many_watch.zk_client_stop()
