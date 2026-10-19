#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Leitor SOMENTE CONSULTA de canais travados do Portal IPBX.

Observação importante:
O Portal IPBX é ASP.NET e exige POSTs de navegação/pesquisa para executar a
consulta. Este módulo NÃO contém o fluxo de limpeza/Confirmar e nunca envia
btnContinue, cbxCheckall ou grdDataSet$DXSelInput.
"""
from __future__ import annotations

import os
import re
import threading
import uuid
from datetime import datetime
from html import unescape
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

IPBX_BASE_PADRAO = "http://10.90.10.101:8181/FORNECEDOR/AppATMAACME/FornecedorPortalExFrontEndWeb/"
IPBX_USER_ID_PADRAO = "100001"
IPBX_TIMEOUT_PADRAO = 20


def hidden_inputs(html: str):
    soup = BeautifulSoup(html, "html.parser")
    out = {}
    for inp in soup.find_all("input"):
        name = inp.get("name")
        if name and (inp.get("type") or "").lower() == "hidden":
            out[name] = inp.get("value", "")
    return out


def guid_valido(value):
    value = (value or "").strip()
    if not value or value == "0":
        return False
    try:
        uuid.UUID(value)
        return True
    except Exception:
        return False


def env_config(instancias):
    cfg = {}
    for inst in instancias:
        inst = str(inst)
        project = os.getenv(f"ORBE_IPBX_PROJETO_{inst}", "").strip()
        menu = os.getenv(f"ORBE_IPBX_MENU_{inst}", "").strip()
        if project and menu:
            cfg[inst] = {"projeto": project, "menu": menu}
    return cfg


class IPBXStuckReader:
    def __init__(self, usuario, senha, instancia, projeto, menu, base=None, timeout=None):
        self.usuario = usuario
        self.senha = senha
        self.instancia = str(instancia)
        self.projeto = str(projeto)
        self.menu = str(menu)
        self.base = (base or os.getenv("IPBX_BASE_URL", IPBX_BASE_PADRAO)).rstrip("/") + "/"
        self.timeout = int(timeout or os.getenv("IPBX_TIMEOUT_SEGUNDOS", str(IPBX_TIMEOUT_PADRAO)))
        self.return_path = "Views/Default.aspx"
        self.login_path = "Views/Login.aspx"
        self.lock = threading.RLock()
        self._new_session()

    def _new_session(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        })

    def _authed(self):
        names = {c.name.lower() for c in self.session.cookies}
        return ".aspxformsauth" in names or "fornecedorauth" in names

    @staticmethod
    def _login_page(resp):
        return "Login.aspx" in resp.url or 'id="txtDeLogin"' in resp.text or 'name="txtDeLogin"' in resp.text

    def authenticate(self):
        return_url = "/" + urlparse(self.base).path.strip("/") + "/" + self.return_path
        url = urljoin(self.base, self.login_path)
        r = self.session.get(url, params={"ReturnUrl": return_url}, timeout=self.timeout)
        r.raise_for_status()
        form = hidden_inputs(r.text)
        required = ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]
        missing = [x for x in required if x not in form]
        if missing:
            raise RuntimeError("IPBX login sem campos ASP.NET: " + ", ".join(missing))
        form.update({
            "ScriptManager1": "UpdatePanel1|btnOk",
            "__EVENTTARGET": "", "__EVENTARGUMENT": "",
            "hidScreenWidth": "", "hidcaleenHeight": "", "textUrl": "",
            "txtDeLogin": self.usuario, "txtDePassword": self.senha,
            "__ASYNCPOST": "true", "btnOk": "Acessar",
        })
        resp = self.session.post(r.url, data=form, headers={
            "X-Requested-With": "XMLHttpRequest", "X-MicrosoftAjax": "Delta=true",
            "Referer": r.url, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }, timeout=self.timeout)
        resp.raise_for_status()
        if not self._authed():
            check = self.session.get(urljoin(self.base, self.return_path), timeout=self.timeout)
            check.raise_for_status()
            if self._login_page(check):
                raise RuntimeError("IPBX não confirmou autenticação")

    def _select_project(self):
        url = urljoin(self.base, self.return_path)
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        if self._login_page(r):
            raise PermissionError("sessão IPBX expirada")
        form = hidden_inputs(r.text)
        form["__EVENTTARGET"] = "ctl00$cmbProjectList"
        form["__EVENTARGUMENT"] = ""
        form["__LASTFOCUS"] = ""
        form["ctl00$cmbProjectList"] = self.projeto
        resp = self.session.post(url, data=form, headers={
            "Referer": r.url, "Content-Type": "application/x-www-form-urlencoded"
        }, timeout=self.timeout)
        resp.raise_for_status()
        if self._login_page(resp):
            raise PermissionError("sessão IPBX expirou ao selecionar projeto")
        return resp

    def _menu_link(self, html, base_url):
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a"):
            text = " ".join(a.stripped_strings).strip()
            href = unescape(a.get("href") or "").strip()
            if not href or ("Remover Canais Travados" not in text and "Remover Canais Travados" not in href):
                continue
            m = re.search(r'openReport\s*\(\s*["\'][^"\']*["\']\s*,\s*["\'](?P<url>[^"\']+)["\']', href, re.I)
            if not m:
                continue
            abs_url = urljoin(base_url, m.group("url").replace("&amp;", "&"))
            qs = parse_qs(urlparse(abs_url).query)
            menu = (qs.get("pIdMenu") or [""])[0].strip()
            guid = (qs.get("pNuGuid") or [""])[0].strip()
            if menu and menu != self.menu:
                continue
            if guid_valido(guid):
                return abs_url, guid
        return None, None

    @staticmethod
    def _quantity(html):
        soup = BeautifulSoup(html, "html.parser")
        row = soup.find("tr", id=re.compile(r"^grdDataSet_DXDataRow\d+$"))
        if row:
            tds = row.find_all("td")
            if len(tds) >= 2:
                m = re.search(r"\d+", tds[-1].get_text(" ", strip=True))
                if m:
                    return int(m.group())
        # fallback textual
        for tr in soup.find_all("tr"):
            txt = " ".join(tr.stripped_strings)
            nums = re.findall(r"\b\d+\b", txt)
            if nums and "Registros presos" not in txt:
                # Só aceita fallback se a página contém o rótulo esperado.
                if "Registros presos > 30 minutos" in soup.get_text(" ", strip=True):
                    return int(nums[-1])
        return None

    def _query_once(self):
        selected = self._select_project()
        url, guid = self._menu_link(selected.text, selected.url)
        if not url:
            raise RuntimeError(f"Instância {self.instancia}: menu de canais travados não encontrado")
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        if self._login_page(r):
            raise PermissionError("sessão IPBX expirada ao abrir consulta")
        form = hidden_inputs(r.text)
        required = ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]
        missing = [x for x in required if x not in form]
        if missing:
            raise RuntimeError("IPBX consulta sem campos ASP.NET: " + ", ".join(missing))
        form["__EVENTTARGET"] = ""
        form["__EVENTARGUMENT"] = ""
        form["btnQuery"] = "Pesquisar"
        form.setdefault("clientUTCTimeZoneOffsetTime", "")
        form.setdefault("clientLocalTimeZoneOffsetTime", "")
        resp = self.session.post(url, data=form, headers={
            "Referer": r.url, "Content-Type": "application/x-www-form-urlencoded"
        }, timeout=self.timeout, allow_redirects=True)
        resp.raise_for_status()
        if self._login_page(resp):
            raise PermissionError("sessão IPBX expirou durante Pesquisar")
        qty = self._quantity(resp.text)
        if qty is None:
            raise RuntimeError(f"Instância {self.instancia}: quantidade de canais travados não extraída")
        return {
            "instancia": self.instancia,
            "quantidade": int(qty),
            "projeto_id": self.projeto,
            "menu_id": self.menu,
            "guid": guid,
            "consultado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
            "modo": "CONSULTA_SOMENTE",
        }

    def consultar(self):
        with self.lock:
            for attempt in (1, 2):
                try:
                    if not self._authed():
                        self.authenticate()
                    return self._query_once()
                except PermissionError:
                    if attempt == 2:
                        raise
                    self._new_session()
                    self.authenticate()
