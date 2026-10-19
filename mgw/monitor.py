from mgw.nap import NAP


class Monitor:
    def __init__(self, cliente, servidor):
        self.cliente = cliente
        self.servidor = servidor

    def obter_naps(self):
        dados = self.cliente.obter_naps()

        if dados is None:
            return None

        return [
            NAP(item, servidor=self.servidor)
            for item in dados
        ]

    def separar_por_status(self, naps):
        ativos = [
            nap for nap in naps
            if nap.esta_ativo()
        ]

        inativos = [
            nap for nap in naps
            if nap.esta_inativo()
        ]

        return ativos, inativos