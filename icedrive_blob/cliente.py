import Ice

import IceDrive
import hashlib

def test_directory_service():
    with Ice.initialize() as communicator:
        with open("proxy.txt", "r") as f:
            proxy_str = f.read()
        base = communicator.stringToProxy(proxy_str)
        directory_service = IceDrive.DirectoryServicePrx.checkedCast(base)

        # Llama a métodos del servicio de directorio
        root_directory = directory_service.getRoot("usuario")

if __name__ == "__main__":
    # Ejecuta las pruebas
    test_directory_service()
