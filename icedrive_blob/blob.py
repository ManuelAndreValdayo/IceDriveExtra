"""Module for servants implementations."""

import Ice
from pathlib import Path
import IceDrive
import json
import tempfile
import hashlib
import base64
from .delayed_response import BlobQuery, BlobQueryResponse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import time
import pdb
import threading


class DataTransfer(IceDrive.DataTransfer):
    """Implementation of an IceDrive.DataTransfer interface."""
    def __init__(self, filename):
        #Pasar ruta del archivo
        self.fd = open(filename, "rb")
        
    def read(self, size: int, current: Ice.Current = None) -> bytes:
        """Returns a list of bytes from the opened file."""
        try:
            return self.fd.read(size)
        except IceDrive.FailedToReadData as e:
            raise IceDrive.FailedToReadData("Error reading data. " + str(e))

    def close(self, current: Ice.Current = None) -> None:
        """Close the currently opened file."""
        self.fd.close()


class BlobService(IceDrive.BlobService):
    """Implementation of an IceDrive.BlobService interface."""
    def __init__(self, persistencia, ruta_persistencia, blob_query: IceDrive.BlobQueryPrx, adapter):
        self.persistencia = persistencia
        self.ruta_persistencia = f"{ruta_persistencia}.json"
        self.blob_query_publisher = blob_query
        self.adapter = adapter
        self.expected_responses = {}
        
    def remove_object_if_exists(self, adapter: Ice.ObjectAdapter, identity: Ice.Identity) -> None:
        """Remove an object from the adapter if exists."""
        if adapter.find(identity) is not None:
            adapter.remove(identity)
            self.expected_responses[identity].set_exception(IceDrive.TemporaryUnavailable())

        del self.expected_responses[identity]
              
    def crear_blobQueryResponse(self, adapter,  current: Ice.Current = None) -> IceDrive.BlobQueryResponsePrx:
        future = Ice.Future()
        response = BlobQueryResponse(future)
        prx = adapter.addWithUUID(response)
        query_response_prx = IceDrive.BlobQueryResponsePrx.uncheckedCast(prx)
        
        identity = query_response_prx.ice_getIdentity()
        self.expected_responses[identity] = future
        threading.Timer(5.0, self.remove_object_if_exists, (adapter, identity)).start()

        return query_response_prx
    
    def procesar_blob(self, blob):
        # Crear un archivo temporal que se elimine automáticamente al cerrar
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
            sha256 = hashlib.sha256()
            chunk = blob.read(4000)
            contenido = b''
            while chunk:
                try:
                    sha256.update(chunk)  # Actualizar el hash con cada fragmento
                    contenido += chunk
                    chunk = blob.read(4000)
                except IceDrive.FailedToReadData as e:
                    raise IceDrive.FailedToReadData("Error al subir los datos: " + str(e))
        
        # Asegurarse de que el blob se cierre adecuadamente
        try:
            blob.close()
        except Exception as e:
            raise IceDrive.FailedToReadData("Error al cerrar el blob: " + str(e))
        contenido_base64 = base64.b64encode(contenido).decode('utf-8')
        return (contenido_base64, sha256.hexdigest())      
      
    def guardarPersistencia(self):
        with open(self.ruta_persistencia, 'w') as archivo:
            json.dump(self.persistencia, archivo, indent=4)
            
    def link(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id file as linked in some directory."""
        if blob_id in self.persistencia:
            self.persistencia[blob_id][1] += 1
            self.guardarPersistencia()

    def unlink(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id as unlinked (removed) from some directory."""
        if blob_id in self.persistencia:
            self.persistencia[blob_id][1] -= 1
            if self.persistencia[blob_id][1] == 0:
                del self.persistencia[blob_id]
            self.guardarPersistencia()


    def upload(
        self, user: IceDrive.UserPrx, blob: IceDrive.DataTransferPrx, current: Ice.Current = None
    ) -> str:
        """Register a DataTransfer object to upload a file to the service."""

        
        #authentication_proxy = self.services.getAuthenticationService()
        #authentication_service = IceDrive.AuthenticationPrx.checkedCast(authentication_proxy)
        #if authentication_service.verifyUser(user) == True:
        if True:
            file_blob = self.procesar_blob(blob)
            #if file_blob[1] in self.persistencia:
            #    return file_blob[1]
            #else:
            BlobQueryResponse_proxy = self.crear_blobQueryResponse(self.adapter)
            inicio = time.time()
            self.blob_query_publisher.doesBlobExist(file_blob[1],BlobQueryResponse_proxy)
            seguir = True
            BlobQueryResponse_service = IceDrive.BlobQueryResponsePrx.checkedCast(BlobQueryResponse_proxy)
            
            while seguir:
                if (time.time() - inicio >= 5) :
                    seguir = False
            if(self.expected_responses[BlobQueryResponse_proxy.ice_getIdentity()] != 1):
                self.persistencia[file_blob[1]] = [file_blob[0],1]
                self.guardarPersistencia()                      
            else:
                return file_blob[1]
                    
  
    def download(
        self, user: IceDrive.UserPrx, blob_id: str, current: Ice.Current = None
    ) -> IceDrive.DataTransferPrx:
        """Return a DataTransfer objet to enable the client to download the given blob_id."""
        #authentication_proxy = self.services.getAuthenticationService()
        #authentication_service = IceDrive.AuthenticationPrx.checkedCast(authentication_proxy)
        #if authentication_service.verifyUser(user) == True:
        if blob_id in self.persistencia:
            contenido_base64 = self.persistencia[blob_id][0]
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
            
            return IceDrive.DataTransferPrx.uncheckedCast(data_transfer_proxy)
        else:
            # Consultar otras instancias utilizando BlobQuery
            blob_service = BlobService(persistencia,ruta_persistencia)
            blob_service_proxy = adapter.addWithUUID(blob_service)
            self.blob_service_proxy = blob_service_proxy

            logging.info("Proxy del servicio Blob: %s", blob_service_proxy)
            blob_query_proxy = self.services.getBlobQueryService()
            blob_query_service = IceDrive.BlobQueryPrx.checkedCast(blob_query_proxy)

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(blob_query_service.queryBlob, blob_id)
                done, not_done = wait([future], timeout=5, return_when=FIRST_COMPLETED)

            if future in done:
                response = future.result()
                if response.found:
                    return response.dataTransfer
                else:
                    raise IceDrive.UnknownBlob(f"Blob with ID {blob_id} not found in any instance")
            else:
                raise IceDrive.UnknownBlob(f"Blob with ID {blob_id} not found in any instance")
