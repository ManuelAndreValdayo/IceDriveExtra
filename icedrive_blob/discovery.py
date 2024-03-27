"""Servant implementations for service discovery."""

import Ice

import IceDrive


class Discovery(IceDrive.Discovery):
    """Servants class for service discovery."""

    def announceAuthentication(self, prx: IceDrive.AuthenticationPrx, current: Ice.Current = None) -> None:
        """Receive an Authentication service announcement."""

    def announceDirectoryServicey(self, prx: IceDrive.DirectoryServicePrx, current: Ice.Current = None) -> None:
        """Receive an Directory service announcement."""

    def announceBlobService(self, prx: IceDrive.BlobServicePrx, current: Ice.Current = None) -> None:
        """Receive an Blob service announcement."""

    def getAuthenticationServices(self, current: Ice.Current = None) -> list[IceDrive.AuthenticationPrx]:
        """Return a list of the discovered Authentication*"""

    def getDiscoveryServices(self, current: Ice.Current = None) -> list[IceDrive.DirectoryServicePrx]:
        """Return a list of the discovered DirectoryService*"""

    def getBlobServices(self, current: Ice.Current = None) -> list[IceDrive.BlobServicePrx]:
        """Return a list of the discovered BlobService*"""
