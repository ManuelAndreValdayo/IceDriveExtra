"""Module for servants implementations."""

import Ice

import IceDrive
import json


class DataTransfer(IceDrive.DataTransfer):
    """Implementation of an IceDrive.DataTransfer interface."""

    def read(self, size: int, current: Ice.Current = None) -> bytes:
        """Returns a list of bytes from the opened file."""

    def close(self, current: Ice.Current = None) -> None:
        """Close the currently opened file."""


class BlobService(IceDrive.BlobService):
    """Implementation of an IceDrive.BlobService interface."""
    def __init__(self, persistencia, ruta_persistencia):
        self.persistencia = persistencia
        self.ruta_persistencia = f"{ruta_persistencia}.json"
        
    def guardarPersistencia(self):
        with open(self.ruta_persistencia, 'w') as archivo:
            json.dump(self.persistencia, archivo, indent=4)
            
    def link(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id file as linked in some directory."""

    def unlink(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id as unlinked (removed) from some directory."""

    def upload(
        self, user: IceDrive.UserPrx, blob: IceDrive.DataTransferPrx, current: Ice.Current = None
    ) -> str:
        """Register a DataTransfer object to upload a file to the service."""

    def download(
        self, user: IceDrive.UserPrx, blob_id: str, current: Ice.Current = None
    ) -> IceDrive.DataTransferPrx:
        """Return a DataTransfer objet to enable the client to download the given blob_id."""
