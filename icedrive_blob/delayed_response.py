"""Servant implementation for the delayed response mechanism."""

import Ice

import IceDrive
import logging

DEBUG = logging.debug


class BlobQueryResponse(IceDrive.BlobQueryResponse):
    def __init__(self,future: Ice.Future):
        """Create a BlobQueryResponse object."""
        self.future_callback = future
        
    """Query response receiver."""
    def downloadBlob(self, blob: IceDrive.DataTransferPrx, current: Ice.Current = None) -> None:
        """Receive a `DataTransfer` when other service instance knows the `blob_id`."""

    def blobExists(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and it's stored there."""
        self.future_callback.set_result(1)
        
    def blobLinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was linked."""

    def blobUnlinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was unlinked."""
    

class BlobQuery(IceDrive.BlobQuery):
    def __init__(self,blob_api, discovery_prx):
        self.blob_api = blob_api
        self.discovery_prx = discovery_prx

    """Query receiver."""
    def downloadBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query for downloading an archive based on `blob_id`."""

    def doesBlobExist(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to check if a given `blob_id` is stored in the instance."""
        if blob_id in self.blob_api.persistencia:
            response.blobExists()
        else:
            # Consultar otras instancias
            for service in self.discovery_prx.getBlobServices():
                try:
                    other_response = IceDrive.BlobQueryResponsePrx.uncheckedCast(service)
                    if other_response.doesBlobExist(blob_id):
                        response.blobExists()
                        return
                except Exception as e:
                    continue
            response.blobNotExist()

    def linkBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to create a link for `blob_id` archive if it exists."""

    def unlinkBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to destroy a link for `blob_id` archive if it exists."""
    
    
