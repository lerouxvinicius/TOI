from bs4 import BeautifulSoup


class MGWParser:

    def __init__(self, html):
        self.html = html

        self.soup = BeautifulSoup(html, "html.parser")

    def obter_titulo(self):

        if self.soup.title:
            return self.soup.title.text

        return None
    
    def obter_formulario_login(self):
        return self.soup.find("form", id="formLogIn")   

    def obter_campos_login(self):

        formulario = self.obter_formulario_login()

        if formulario is None:
            return []

        return formulario.find_all("input")