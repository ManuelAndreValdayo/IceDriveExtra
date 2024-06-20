"""Aplicación del servicio de autenticación."""

import logging
import sys
from typing import List

import Ice
import IceDrive
import IceStorm
from threading import Timer
import random

import time
import os

from .blob import BlobService
from .discovery import Discovery
import uuid
import json
import signal


class BlobApp(Ice.Application):
    """Implementación de Ice.Application para el servicio de autenticación."""

    def __init__(self):
        self.blob_service_proxy = None
        
    def anunciar(self, announcements, srvProxy, timer, diccAuthentication,diccDirectory, diccBlob, current=None):
        # Realiza el anuncio
        announcements.announceBlobService(srvProxy)

        # Función auxiliar para verificar y eliminar objetos inválidos
        invalidos = []
        for obj in diccAuthentication:
            try:
                diccAuthentication[obj].ice_ping()
            except Exception as ex:
                invalidos.append(obj)

        for obj in invalidos:
            diccAuthentication.pop(obj)
            
        # Función auxiliar para verificar y eliminar objetos inválidos            
        invalidos = []
        for obj in diccDirectory:
            try:
                diccDirectory[obj].ice_ping()
            except Exception as ex:
                invalidos.append(obj)

        for obj in invalidos:
            diccDirectory.pop(obj)

        # Función auxiliar para verificar y eliminar objetos inválidos
        invalidos = []
        for obj in diccBlob:
            try:
                diccBlob[obj].ice_ping()
            except Exception as ex:
                invalidos.append(obj)

        for obj in invalidos:
            diccBlob.pop(obj)
            
            

        # Programa el siguiente anuncio con un tiempo aleatorio entre 8 segundos
        timer = Timer(8.00, self.anunciar, (announcements, srvProxy, timer, diccAuthentication,diccDirectory, diccBlob))
        timer.start()
        
    def get_topic_manager_proxy(self):
        """Recupera el proxy del TopicManager de IceStorm."""
        topic_manager_key = 'IceStorm.TopicManager.Proxy'
        topic_manager_proxy = self.communicator().propertyToProxy(topic_manager_key)
        if topic_manager_proxy is None:
            print(f"Propiedad '{topic_manager_key}' no configurada")
            return None

        print(f"Usando IceStorm en: '{topic_manager_key}'")
        return IceStorm.TopicManagerPrx.checkedCast(topic_manager_proxy)
    
    def carga_persistencia_inicial(self):
        ruta_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_padre = os.path.dirname(ruta_actual)
        filename = f"{ruta_carpeta_padre}{os.sep}ini.txt"
        while os.path.exists(filename):
            # Comprobar si el archivo existe
            print(f"{filename} otro microservicio se está iniciando...")
            # Esperar un segundo antes de la siguiente comprobación
            time.sleep(5)
            
        print("cargando persistencia")
        contenido = "Archivo de bloqueo."
        with open(filename, 'w') as archivo:
            archivo.write(contenido)
            
        ruta_carpeta = f"{ruta_carpeta_padre}{os.sep}persistencia"
        
        if not os.path.exists(ruta_carpeta):
            os.makedirs(ruta_carpeta)
            
        archivo_elegido = ""
            
        lista_archivos = os.listdir(ruta_carpeta)
        if len(lista_archivos) != 0:
            archivos_json = [f for f in lista_archivos if os.path.isfile(os.path.join(ruta_carpeta, f)) and f.endswith('.json')]
            for archivo in archivos_json:
                nombre_archivo = archivo.split(".")[0]
                if not os.path.exists(f"{ruta_carpeta}{os.sep}{nombre_archivo}.txt"):
                    archivo_elegido = nombre_archivo
                
        persistencia_cargada = {}
        if archivo_elegido == "":
            uuid1 = uuid.uuid1()
            archivo_elegido = f"persistencia_{uuid1}"
            data = {}
            with open(f"{ruta_carpeta}{os.sep}{archivo_elegido}.json", 'w') as archivo:
                json.dump(data,archivo,indent=4)
        else:
            with open(f"{ruta_carpeta}{os.sep}{archivo_elegido}.json", 'r') as archivo:
                persistencia_cargada = json.load(archivo) 
                       
        contenido = "Archivo de bloqueo."
        with open(f"{ruta_carpeta}{os.sep}{archivo_elegido}.txt", 'w') as archivo:
            archivo.write(contenido)
                   
        if os.path.exists(filename):
            os.remove(filename)
            print(f"El archivo '{filename}' ha sido borrado.")
        else:
            print(f"El archivo '{filename}' no existe.")
            
        return (persistencia_cargada,f"{ruta_carpeta}{os.sep}{archivo_elegido}")
        #Queda tocar lo que retorna en el run, borrar el TXT y guardar el JSON al final del RUN cuando muere el microservicio, 
        # y hacer test para ver si no da error lo de abrir y crear archivos sin estar dentro de las carpetas, 
        # por lo del error en la ruta del open que suele pasar
        
    def handler(self,signum, frame,blob_service,ruta_persistencia):
        blob_service.guardarPersistencia()
        #De esta forma liberamos la persistencia que se ha asignado al principio
        ruta_bloqueo = f"{ruta_persistencia}.txt"
        if os.path.exists(ruta_bloqueo):
            os.remove(ruta_bloqueo)
            print(f"El archivo '{ruta_bloqueo}' ha sido borrado.")
        else:
            print(f"El archivo '{ruta_bloqueo}' no existe.")
            
        self.shutdownOnInterrupt()  
        self.communicator().shutdown()
        os.kill(os.getpid(), signal.SIGSTOP)


            
    def run(self, args: List[str]) -> int:
        authentication_services = {}
        directory_services = {}
        blob_services = {}
        """Ejecuta la lógica principal de la clase BlobApp."""
        communicator = self.communicator()

        # Crear un adaptador de objetos para el BlobService
        adapter = communicator.createObjectAdapter("BlobAdapter")

        # Recuperar el proxy del TopicManager de IceStorm
        topic_manager_proxy = self.get_topic_manager_proxy()
        if topic_manager_proxy is None:
            print("No hay proxy del TopicManager")
            return 0

        # Crear y añadir un servidor BlobService al adaptador
        
        #Creamos el archivo init.txt 
        resultado_carga_persistencia = self.carga_persistencia_inicial()
        persistencia = resultado_carga_persistencia[0]
        ruta_persistencia = resultado_carga_persistencia[1]
        
        blob_service = BlobService(persistencia,ruta_persistencia)
        blob_service_proxy = adapter.addWithUUID(blob_service)
        self.blob_service_proxy = blob_service_proxy

        logging.info("Proxy del servicio Blob: %s", blob_service_proxy)
        
        with open("proxy.txt", "w") as f:
            f.write(str(blob_service_proxy))
        # Crear un servidor Discovery y añadirlo al adaptador
        discovery_service = Discovery(authentication_services,directory_services,blob_services)
        discovery_service_proxy = adapter.addWithUUID(discovery_service)

        # Configurar el tema de IceStorm para Discovery
        qos = {}
        try:
            discovery_topic = topic_manager_proxy.retrieve("Discovery")
        except IceStorm.NoSuchTopic:
            discovery_topic = topic_manager_proxy.create("Discovery")
        
        discovery_topic.subscribeAndGetPublisher(qos, discovery_service_proxy)
        print(f"Esperando eventos... '{discovery_service_proxy}'")
        
        blob_query_publisher_proxy = IceDrive.BlobQueryPrx.uncheckedCast(discovery_topic)
        print(blob_query_publisher_proxy)
        
        discovery_publisher = discovery_topic.getPublisher()
        discovery_proxy = IceDrive.DiscoveryPrx.uncheckedCast(discovery_publisher)
        blob_service_proxy_cast = IceDrive.BlobServicePrx.uncheckedCast(blob_service_proxy)

        # Activar el adaptador
        adapter.activate()

        # Anunciar el BlobService al servicio Discovery
        #discovery_proxy.announceBlobService(blob_service_proxy_cast)
        

        
        timer = None
        timer = Timer(3.00,self.anunciar,(discovery_proxy, blob_service_proxy_cast,timer, authentication_services,directory_services,blob_services))
        timer.start()

        
        # Esperar la señal de interrupción para cerrar el comunicador
        #self.shutdownOnInterrupt()
        #communicator.waitForShutdown()
        

        signal.signal(signal.SIGTSTP, lambda signum, frame: self.handler(signum, frame, blob_service, ruta_persistencia))
                
        while True:
            print("Ejecutando...")
            time.sleep(8)    
            
        #return 0


def main():
    """Punto de entrada para el programa icedrive-authentication."""
    app = BlobApp()
    return app.main(sys.argv)

if __name__ == "__main__":
    main()
