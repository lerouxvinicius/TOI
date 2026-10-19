class Estatisticas:

    def __init__(self, naps):
        self.naps = naps

    def total(self):
        return len(self.naps)

    def ativos(self):
        return len([n for n in self.naps if n.esta_ativo()])

    def inativos(self):
        return len([n for n in self.naps if n.esta_inativo()])

    def ocupados(self):
        return len([n for n in self.naps if n.busy > 0])

    def livres(self):
        return len([n for n in self.naps if n.idle > 0])