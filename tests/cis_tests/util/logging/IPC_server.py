import argparse
from pathlib import Path
import socket
import os
import pickle
import struct
import logging

from snapred.backend.log.logger import snapredLogger, CustomFormatter, getIPCSocketPath
from snapred.meta.Config import Config

class ClientDisconnected(Exception):
    pass

class IPCServer:
    _SNAPRed_handler_name = Config["logging.SNAP.stream.handler.name"]
    
    def __init__(self, name: str, PID: int | str):
        self._socket_path: Path = getIPCSocketPath(name, PID)
        self._logger = snapredLogger.getLogger(name)
        
        # Associate a logger with this server _instance_, not with its _module_!
        formatter = CustomFormatter(name=f"IPC.handlers.{name}")
        for handler in self._logger.handlers:
            if handler.name == self._SNAPRed_handler_name:
                handler.setFormatter(formatter)

    @property
    def socket_path(self) -> Path:
        return self._socket_path
                
    def receive_exact(self, conn, count):
        buf = b''
        while len(buf) < count:
            chunk = conn.recv(count - len(buf))
            if not chunk:
                raise ClientDisconnected('Client disconnected')
            buf += chunk
        return buf

    def run(self):
        # Clean up any existing socket
        if self.socket_path.exists():
            self._logger.warning(f"Removing existing socket at '{self.socket_path}'.")
            socketPath.unlink()
            
        server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_sock.bind(str(self.socket_path))
        server_sock.listen(1)
        self._logger.info(f"Listening on {self.socket_path}")

        try:
            while True:
                conn, _ = server_sock.accept()
                print("Client connected.")
                try:
                    while True:
                        # First, read 4-byte length prefix
                        length_bytes = self.receive_exact(conn, 4)
                        slen = struct.unpack('>L', length_bytes)[0]
                        # Then, read the pickled LogRecord
                        pickled_data = self.receive_exact(conn, slen)
                        log_record_dict = pickle.loads(pickled_data)
                        record = logging.makeLogRecord(log_record_dict)
                        self._logger.handle(record)
                except ClientDisconnected:
                    self._logger.info("Client disconnected.")
                except (struct.error, pickle.UnpicklingError):
                    self._logger.error("Protocol error.")
                finally:
                    conn.close()
        finally:
            server_sock.close()
            self.socket_path.unlink()

if __name__ == "__main__":
    try:
        # Parse the command line arguments.
        parser = argparse.ArgumentParser()
        parser.add_argument("-n", "--name", help="the name of the SNAPRed IPC-handler to log: see 'application.yml'")
        parser.add_argument("-p", "--pid", help="the PID of the SNAPRed process")
        args = parser.parse_args()
        name, PID = args.name, args.pid
        
        # Create and start the IPC server.
        server = IPCServer(name, PID)
        server.run()
    except KeyboardInterrupt:
        print("\nServer shutting down.")
