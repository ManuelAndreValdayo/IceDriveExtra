import Ice

import importlib.util
import hashlib
import os
import sys

import IceDrive

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
        
class TestApp(Ice.Application):
    """Implementation of the Ice.Application for the Authentication service."""
    
    def test_upload(self,adapter,blob_service, aux_service):
        ruta_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_padre = os.path.dirname(ruta_actual)
        filename = f"{ruta_carpeta_padre}{os.sep}contenido.txt"
        # Llama a métodos del servicio de directorio
        dt = DataTransfer(filename)
        data = adapter.addWithUUID(dt)
        dt_proxy = IceDrive.DataTransferPrx.uncheckedCast(data)
        adapter.activate()
        upload_result = blob_service.upload(aux_service,dt_proxy)
        print(upload_result)

    def test_download(self,blob_service,aux_service, blob_id):
        # Download the file
        ruta_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_padre = os.path.dirname(ruta_actual)
        downloaded_dt_proxy = blob_service.download(aux_service, blob_id)
        
        # Create a DataTransfer object to read the content
        downloaded_filename = f"{ruta_carpeta_padre}{os.sep}downloaded_contenido.txt"
        with open(downloaded_filename, "wb") as f:
            size = 4000
            while True:
                chunk = downloaded_dt_proxy.read(size)
                if not chunk:
                    break
                f.write(chunk)
        
        # Read and display the content of the downloaded file
        with open(downloaded_filename, "rb") as f:
            content = f.read()
            print("Downloaded content:")
            print(content.decode('utf-8'))
            
    def link_blob(self, blob_id: str, blob_service) -> None:
        """Link a blob in the BlobService."""
        blob_service.link(blob_id)
        print(f"Linked blob ID: {blob_id}")

    def unlink_blob(self, blob_id: str, blob_service) -> None:
        """Unlink a blob in the BlobService."""
        blob_service.unlink(blob_id)
        print(f"Unlinked blob ID: {blob_id}")
                    
    def run(self, args: list[str]) -> int:
        try:
            communicator = self.communicator()
            
            adapter = communicator.createObjectAdapter("BlobAdapter")

            with open("proxy.txt", "r") as f:
                proxy_str = f.read()
            base = communicator.stringToProxy(proxy_str)
            blob_service = IceDrive.BlobServicePrx.checkedCast(base)
            aux_service = IceDrive.UserPrx.checkedCast(base)
            self.test_upload(adapter,blob_service, aux_service)
            #blob_id = '56293a80e0394d252e995f2debccea8223e4b5b2b150bee212729b3b39ac4d46'
            #self.test_download(blob_service,aux_service, blob_id)
            #self.unlink_blob(blob_id,blob_service)
            #blob_service.upload(aux_service,dt_proxy)


        except Exception as e:
            print(e)


def main():
    """Handle the icedrive-authentication program."""
    app = TestApp()
    return app.main(sys.argv)

if __name__ == "__main__":
    main()