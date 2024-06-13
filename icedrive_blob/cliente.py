import Ice

import importlib.util
import hashlib
import os

if importlib.util.find_spec("IceDrive") is None:
    slice_path = os.path.join(os.path.dirname(__file__), "icedrive.ice")
    if not os.path.exists(slice_path):
        raise ImportError("Cannot find icedrive.ice for loading IceDrive module")

    Ice.loadSlice(slice_path)

import IceDrive

def test_blob_service():
    with Ice.initialize() as communicator:
        with open("proxy.txt", "r") as f:
            proxy_str = f.read()
        base = communicator.stringToProxy(proxy_str)
        blob_service = IceDrive.BlobServicePrx.checkedCast(base)

        # Llama a métodos del servicio de directorio
        blob_service.link(base)

if __name__ == "__main__":
    # Ejecuta las pruebas
    test_blob_service()
