"""Authentication service application."""

import logging
import sys
from typing import List

import Ice

from .blob import BlobService
from .discovery import Discovery
import IceDrive
import IceStorm
from threading import Timer
import random


class BlobApp(Ice.Application):
    """Implementation of the Ice.Application for the Authentication service."""
    def __init__(self):
        self.proxy = None

    def get_topic_manager(self):
        key = 'IceStorm.TopicManager.Proxy'
        proxy = self.communicator().propertyToProxy(key)
        if proxy is None:
            print("property '{}' not set".format(key))
            return None

        print("Using IceStorm in: '%s'" % key)
        return IceStorm.TopicManagerPrx.checkedCast(proxy)
    
    def run(self, args: List[str]) -> int:
        """Execute the code for the BlobApp class."""
        broker = self.communicator()
        adapter = broker.createObjectAdapter("BlobAdapter")
        discovery_proxy = self.get_topic_manager()
        if discovery_proxy is None:
            print("No proxy")
            return 0
        # adminToken = broker.getProperties().getProperty('AdminToken')
        # blob = BlobService()
        # adapter.activate()

        servant = BlobService()
        servant_proxy = adapter.addWithUUID(servant)
        self.proxy = servant_proxy

        # with open("proxy.txt", "w") as f:
        #     f.write(servant_proxy)

        logging.info("Proxy: %s", servant_proxy)

        # # Create a Discovery servant and add it to the adapter
        # discovery_servant = Discovery()
        # adapter.add(discovery_servant, self.communicator().stringToIdentity("Discovery"))
        discovery = Discovery()
        ms_Discovery = adapter.addWithUUID(discovery)

        qos = {}
        try:
            DiscoveryBlob_topic = discovery_proxy.retrieve("Discovery")
        except IceStorm.NoSuchTopic:
            DiscoveryBlob_topic = discovery_proxy.create("Discovery")
        DiscoveryBlob_topic.subscribeAndGetPublisher(qos, ms_Discovery)
        print("Waiting events... '{}'".format(ms_Discovery))
        
        query_publisher_proxy = IceDrive.BlobQueryPrx.uncheckedCast(DiscoveryBlob_topic)
        print(query_publisher_proxy)
        DiscoveryBlob_publisher = DiscoveryBlob_topic.getPublisher()
        DiscoveryBlol_prx = IceDrive.DiscoveryPrx.uncheckedCast(DiscoveryBlob_publisher)
        proxyBlob = IceDrive.BlobServicePrx.uncheckedCast(servant_proxy)
        adapter.activate()
        DiscoveryBlol_prx.announceBlobService(proxyBlob)
        #ListaBlob = DiscoveryBlol_prx.getBlobServices()
        #print(ListaBlob)
        proxy_str = str(servant_proxy)
        with open("proxy.txt", "w") as f:
            f.write(proxy_str)
            
        self.shutdownOnInterrupt()
        self.communicator().waitForShutdown()

        return 0


def main():
    """Handle the icedrive-authentication program."""
    app = BlobApp()
    return app.main(sys.argv)

if __name__ == "__main__":
    main()
