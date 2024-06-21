"""Servant implementation for the delayed response mechanism."""

import Ice

import IceDrive
import logging
import threading
from .blob import DataTransfer
import tempfile
import base64




DEBUG = logging.debug


class BlobQueryResponse(IceDrive.BlobQueryResponse):
    def __init__(self,future: Ice.Future):
        """Create a BlobQueryResponse object."""
        self.future = future
        
    """Query response receiver."""
    def downloadBlob(self, blob: IceDrive.DataTransferPrx, current: Ice.Current = None) -> None:
        """Receive a `DataTransfer` when other service instance knows the `blob_id`."""
        self.future.set_result(blob)
        

    def blobExists(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and it's stored there."""
        self.future.set_result(1)
        
    def blobLinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was linked."""
        self.future.set_result(1)


    def blobUnlinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was unlinked."""
        self.future.set_result(1)

    

class BlobQuery(IceDrive.BlobQuery):
    def __init__(self,blob_api):
        self.blob_api = blob_api

    """Query receiver."""
    def downloadBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query for downloading an archive based on `blob_id`."""
        if blob_id in self.blob_api.persistencia:
            contenido_base64 = self.blob_api.persistencia[blob_id][0]
            contenido_bytes = base64.b64decode(contenido_base64)

            # Crear un archivo temporal para almacenar el contenido decodificado
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(contenido_bytes)
                temp_file_path = temp_file.name

            # Crear una instancia de DataTransfer con la ruta del archivo temporal
            data_transfer = DataTransfer(temp_file_path)
            
            # Adaptador de objeto para devolver el proxy del DataTransfer
            communicator = current.adapter.getCommunicator()
            adapter = communicator.createObjectAdapterWithEndpoints("DataTransferAdapter", "tcp -h localhost -p 0")
            data_transfer_proxy = adapter.addWithUUID(data_transfer)
            adapter.activate()
            
            consulta_hilo = threading.Thread(target=response.downloadBlob, args=(data_transfer_proxy))
            consulta_hilo.start()

    def doesBlobExist(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to check if a given `blob_id` is stored in the instance."""
        if blob_id in self.blob_api.persistencia:
            consulta_hilo = threading.Thread(target=response.blobExists)
            consulta_hilo.start()
            #response.blobExists()

    def linkBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to create a link for `blob_id` archive if it exists."""
        if blob_id in self.blob_api.persistencia:
            self.blob_api.persistencia[blob_id][1]+=1
            self.blob_api.guardarPersistencia()
            consulta_hilo = threading.Thread(target=response.blobLinked)
            consulta_hilo.start()

    def unlinkBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to destroy a link for `blob_id` archive if it exists."""
        if blob_id in self.blob_api.persistencia:
            self.blob_api.persistencia[blob_id][1]-=1
            if self.blob_api.persistencia[blob_id][1] < 1:
                del self.blob_api.persistencia[blob_id][1]
            self.blob_api.guardarPersistencia()
            consulta_hilo = threading.Thread(target=response.blobUnlinked)
            consulta_hilo.start()
    
    
