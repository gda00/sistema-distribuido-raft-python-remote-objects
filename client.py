import Pyro5.api
import time

class RaftCliente:
    def __init__(self):
        self.leader = None
    
    def _find_leader(self):
        try:
            ns = Pyro5.api.locate_ns()
            uri = ns.lookup("Leader")
            self.leader = Pyro5.api.Proxy(uri)
            print (f"[CLIENTE] Líder encontrado na URI: {uri}")
            return True
        except Exception:
            print(f"[CLIENTE] Líder não encontrado no NameServer :( ")
            self.leader = None
            return False
    
    def send_command(self, command):
        if not self.leader:
            if not self._find_leader():
                return False

        try:
            self.leader.receive_command(command)
            print(f"[CLIENTE] Comando '{command}' enviado com sucesso")
            return True
        except Exception:
            print(f"[CLIENTE] Falha ao enviar o comando: {Exception}")
            self.leader = None
            return False
        
    def run(self):
        print("*******Cliente Iniciado*******")
        print("digite o comando ou 'sair' para encerrar!")

        while True:
            command = input("> ")
            if command.lower() == 'sair':
                break

            if command.strip():
                sucesso = self.send_command(command)

                while not sucesso:
                    time.sleep(2)
                    sucesso = self.send_command(command)
                
if __name__ == "__main__":
    cliente = RaftCliente()
    cliente.run()