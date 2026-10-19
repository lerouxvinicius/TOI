class NAP:
    def __init__(self, dados, servidor=""):
        self._dados = dados

        self.servidor = servidor
        self.id = dados.get("Id")
        self.nome = dados.get("Name", "")
        self.ativo = dados.get("Active", False)
        self.tipo = dados.get("Type")

        canal = dados.get("Channel", {})

        self.busy = canal.get("Busy", 0)
        self.idle = canal.get("Idle", 0)
        self.fail = canal.get("Fail", 0)

        self.proxy = None
        self.ip = None
        self.porta = None
        self.alive = False

        sip = dados.get("SIP", {})
        proxies = sip.get("ProxyList", [])

        if proxies:
            primeiro_proxy = proxies[0]

            self.proxy = primeiro_proxy.get("Address")
            self.alive = primeiro_proxy.get("IsAlive", False)

            if self.proxy and "]:" in self.proxy:
                ip, porta = self.proxy.replace("[", "").split("]:")

                self.ip = ip

                try:
                    self.porta = int(porta)
                except ValueError:
                    self.porta = None

    def esta_ativo(self):
        return self.ativo

    def esta_inativo(self):
        return not self.ativo

    def tem_falhas(self):
        return self.fail > 0

    def endereco(self):
        if self.ip and self.porta:
            return f"{self.ip}:{self.porta}"

        return "Não informado"

    def resumo(self):
        return (
            f"{self.servidor} | "
            f"{self.nome} | "
            f"Ativo={self.ativo} | "
            f"Busy={self.busy} | "
            f"Idle={self.idle}"
        )

    def __str__(self):
        return self.resumo()