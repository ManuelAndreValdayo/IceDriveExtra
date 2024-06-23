# IceDrive Authentication service template

This repository contains the project template for the Blob service proposed as laboratory work for the students
of Distributed Systems in the course 2023-2024.

## Updating `pyproject.toml`

One of the first things that you need to setup when you clone this branch is to take a look into
`pyproject.toml` file and update the needed values:

- Project authors MANUEL ANDRÉ VALDAYO BRICAUD
- Project dependencies
- Projects URL humepage
  
En primer lugar es necesario crear los tópicos y para ello tendremos que ejecutar en una terminal ./run_icestorm

Para poder arrancar el servicio es necesario abrir la terminal e introducir el siguiente comando:

    python3.11 -m icedrive_blob.app --Ice.Config=./config/blob.config

Es recomendable eliminar la carpeta persistencia cuando se quiera probar el servicio para que se vuelva a crear de forma automática.

Para poder ejecutar el cliente utilizo el siguiente comando:
    
    python3.11 -m icedrive_blob.cliente --Ice.Config=./config/blob.config

    No me dió tiempo a poder implementar un cliente con una mínima interfaz por lo que simplemente tengo comentado las funciones del blobservice que no quiero utilizar 
    y descomentadas las que si quiero utilizar.

