"""Module for servants implementations."""

import Ice
from pathlib import Path
import IceDrive
import json
import tempfile
import hashlib
import base64
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import time
import pdb
import threading
import random

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
    def __init__(self, persistencia, ruta_persistencia, blob_query: IceDrive.BlobQueryPrx, adapter,authentication_services):
        self.persistencia = persistencia
        self.ruta_persistencia = f"{ruta_persistencia}.json"
        self.blob_query_publisher = blob_query
        self.adapter = adapter
        self.expected_responses = {}
        self.authentication_services = authentication_services

    def remove_object_if_exists(self, adapter: Ice.ObjectAdapter, identity: Ice.Identity) -> None:
        """Remove an object from the adapter if exists."""
        if adapter.find(identity) is not None:
            adapter.remove(identity)
            self.expected_responses[identity].set_exception(IceDrive.TemporaryUnavailable())

        del self.expected_responses[identity]

    def crear_blobQueryResponse(self, adapter, future,current: Ice.Current = None) -> IceDrive.BlobQueryResponsePrx:
        from .delayed_response import BlobQueryResponse
        response = BlobQueryResponse(future)
        prx = adapter.addWithUUID(response)
        query_response_prx = IceDrive.BlobQueryResponsePrx.uncheckedCast(prx)
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
    
    def ControlarFuture(self, future, result_container):
        try:
            result = future.result()  # Bloquea hasta que el future tenga un resultado
            if result == 1:
                print("El blob existe en el servicio.")
            else:
                print("El blob no existe.")
                
            result_container[0] = future
            
        except Ice.Exception as e:
            print(f"Se produjo una excepción: {e}")
        

    def link(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id file as linked in some directory."""
        if blob_id in self.persistencia:
            self.persistencia[blob_id][1] += 1
            self.guardarPersistencia()
        else:
            future = Ice.Future()
            inicio = time.time()
            result_container = [future]
            BlobQueryResponse_proxy = self.crear_blobQueryResponse(self.adapter, future)
            self.blob_query_publisher.linkBlob(blob_id,BlobQueryResponse_proxy)
            consulta_hilo = threading.Thread(target=self.ControlarFuture, args=(future, result_container))
            consulta_hilo.start()
            seguir = True
            while seguir:
                if (time.time() - inicio >= 5) :
                    seguir = False
            
            future_result = result_container[0]
            if future_result.done():
                raise IceDrive.UnknownBlob("Error al linkear el blob: " + blob_id)            
            else:
                return blob_id

    def unlink(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id as unlinked (removed) from some directory."""
        if blob_id in self.persistencia:
            self.persistencia[blob_id][1] -= 1
            if self.persistencia[blob_id][1] == 0:
                del self.persistencia[blob_id]
            self.guardarPersistencia()
        else:
            future = Ice.Future()
            inicio = time.time()
            result_container = [future]
            BlobQueryResponse_proxy = self.crear_blobQueryResponse(self.adapter, future)
            self.blob_query_publisher.unlinkBlob(blob_id,BlobQueryResponse_proxy)
            consulta_hilo = threading.Thread(target=self.ControlarFuture, args=(future, result_container))
            consulta_hilo.start()
            seguir = True
            while seguir:
                if (time.time() - inicio >= 5) :
                    seguir = False
            
            future_result = result_container[0]
            if future_result.done():
                raise IceDrive.UnknownBlob("Error al unlinkear el blob: " + blob_id)            
            else:
                return blob_id


    def upload(
        self, user: IceDrive.UserPrx, blob: IceDrive.DataTransferPrx, current: Ice.Current = None
    ) -> str:
        """Register a DataTransfer object to upload a file to the service."""

        clave_aleatoria = random.choice(list(self.authentication_services.keys()))
        authentication_proxy = self.authentication_services[clave_aleatoria]

        authentication_service = IceDrive.AuthenticationPrx.checkedCast(authentication_proxy)
        if authentication_service.verifyUser(user) == True:
            file_blob = self.procesar_blob(blob)
            if file_blob[1] in self.persistencia:
                return file_blob[1]
            else:
                future = Ice.Future()
                inicio = time.time()
                result_container = [future]
                BlobQueryResponse_proxy = self.crear_blobQueryResponse(self.adapter, future)
                self.blob_query_publisher.doesBlobExist(file_blob[1],BlobQueryResponse_proxy)
                consulta_hilo = threading.Thread(target=self.ControlarFuture, args=(future, result_container))
                consulta_hilo.start()
                seguir = True
                while seguir:
                    if (time.time() - inicio >= 5) :
                        seguir = False
                

                future_result = result_container[0]
                if future_result.done():
                    return file_blob[1]
                else:
                    self.persistencia[file_blob[1]] = [file_blob[0], 1]
                    self.guardarPersistencia()
                    return 'No existe en niguna persistencia. Uploading...'
        else:
            return 


    def download(
        self, user: IceDrive.UserPrx, blob_id: str, current: Ice.Current = None
    ) -> IceDrive.DataTransferPrx:
        """Return a DataTransfer objet to enable the client to download the given blob_id."""
        clave_aleatoria = random.choice(list(self.authentication_services.keys()))
        authentication_proxy = self.authentication_services[clave_aleatoria]

        authentication_service = IceDrive.AuthenticationPrx.checkedCast(authentication_proxy)
        if authentication_service.verifyUser(user) == True:
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
                    future = Ice.Future()
                    inicio = time.time()
                    result_container = [future]
                    BlobQueryResponse_proxy = self.crear_blobQueryResponse(self.adapter, future)
                    self.blob_query_publisher.downloadBlob(blob_id,BlobQueryResponse_proxy)
                    consulta_hilo = threading.Thread(target=self.ControlarFuture, args=(future, result_container))
                    consulta_hilo.start()
                    seguir = True
                    while seguir:
                        if (time.time() - inicio >= 5) :
                            seguir = False
                    

                    future_result = result_container[0]
                    data_transfer_proxy = future_result.result()
                    if future_result.done():
                        return data_transfer_proxy
                    else:
                        raise IceDrive.UnknownBlob("Error al descargar el blob, no existe en la persistencia de ningún microservicio " + blob_id) 
