"""Servant implementations for service discovery."""

import Ice

import IceDrive
from typing import List


class Discovery(IceDrive.Discovery):
    """Servants class for service discovery."""
    def __init__(self):
        self.authentication_services = {}
        self.directory_services = {}
        self.blob_services = {}

    def announceAuthentication(self, prx: IceDrive.AuthenticationPrx, current: Ice.Current = None) -> None:
        """Receive an Authentication service announcement."""
        print("servicio autentication anunciado")
        self.authentication_services[prx.ice_getIdentity()] = prx

    def announceDirectoryServicey(self, prx: IceDrive.DirectoryServicePrx, current: Ice.Current = None) -> None:
        """Receive an Directory service announcement."""
        print("servicio directory anunciado")
        self.directory_services[prx.ice_getIdentity()] = prx

    def announceBlobService(self, prx: IceDrive.BlobServicePrx, current: Ice.Current = None) -> None:
        """Receive an Blob service announcement."""
        print("servicio blob anunciado")
        self.blob_services[prx.ice_getIdentity()] = prx

    def getAuthenticationServices(self, current: Ice.Current = None) -> list[IceDrive.AuthenticationPrx]:
        """Return a List of the discovered Authentication*"""
        return list(self.authentication_services.values())

    def getDiscoveryServices(self, current: Ice.Current = None) -> list[IceDrive.DirectoryServicePrx]:
        """Return a List of the discovered DirectoryService*"""
        return list(self.directory_services.values())

    def getBlobServices(self, current: Ice.Current = None) -> list[IceDrive.BlobServicePrx]:
        """Return a List of the discovered BlobService*"""
        return list(self.blob_services.values())
