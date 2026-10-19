import pandas as pd
import requests
import re
import time
from html import unescape
from pathlib import Path
import unicodedata

# Arquivo de entrada e saída
ARQUIVO_ENTRADA = "telefones.xls"
ARQUIVO_SAIDA = "telefones_validados_consultaoperadora.xlsx"

def limpar_e_formatar_numero(val):
    """
    Limpa o número deixando apenas dígitos e valida a estrutura do Brasil.
    """
    if pd.isna(val):
        return None, "Inválido / Vazio", "N/A", "N/A"
    
    # Mantém apenas números
    num_str = re.sub(r'\D', '', str(val))
    
    # Remove prefixo 55 se já existir
    if num_str.startswith("55") and len(num_str) in [12, 13]:
        num_str = num_str[2:]
        
    # Validação do tamanho (DDD + Número)
    if len(num_str) not in [10, 11]:
        return num_str, "Inválido / Sintaxe Incorreta", "Desconhecido", "N/A"
    
    ddd = int(num_str[:2])
    ddds_validos = [
        11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 24, 27, 28, 31, 32, 33, 34, 35, 37, 38,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 51, 53, 54, 55, 61, 62, 63, 64, 65, 66, 67, 68,
        69, 71, 73, 74, 75, 77, 79, 81, 82, 83, 84, 85, 86, 87, 88, 89, 91, 92, 93, 94, 95,
        96, 97, 98, 99
    ]
    
    if ddd not in ddds_validos:
        return num_str, "Inválido / DDD Inexistente", "Desconhecido", "N/A"
    
    primeiro_digito = num_str[2]
    
    if len(num_str) == 11 and primeiro_digito == '9':
        tipo = "Móvel"
    elif len(num_str) == 10 and primeiro_digito in ['2', '3', '4', '5']:
        tipo = "Fixo"
    else:
        return num_str, "Inválido / Formato Número", "Desconhecido", "N/A"
        
    return f"+55{num_str}", "Válido", tipo, f"DDD {ddd}"

def consultar_consulta_operadora(numero_limpo):
    """
    Realiza a consulta e extrai exatamente o conteúdo da tag <span class="lead laranja">
    """
    num_somente_digitos = re.sub(r'\D', '', numero_limpo)
    if num_somente_digitos.startswith("55") and len(num_somente_digitos) in [12, 13]:
        num_somente_digitos = num_somente_digitos[2:]

    url = "https://consulta-operadora.example.com/site2015/resposta.php"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://consulta-operadora.example.com",
        "Referer": "https://consulta-operadora.example.com/site2015/"
    }
    
    payload = {
        "tipo": "consulta",
        "numero": num_somente_digitos
    }

    try:
        response = requests.post(url, data=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            html = response.text
            
            # Captura o valor exato no span após "Operadora:"
            match_operadora = re.search(
                r'Operadora:</span>\s*<span[^>]*class=["\']lead laranja["\'][^>]*>\s*([^<]+)</span>', 
                html, 
                re.IGNORECASE
            )
            
            if match_operadora:
                operadora_bruta = match_operadora.group(1).strip()
            else:
                # Fallback caso o site mude ligeiramente a ordem das classes
                match_operadora = re.search(
                    r'Operadora:</span>.*?<span[^>]*>\s*([^<]+)</span>', 
                    html, 
                    re.IGNORECASE | re.DOTALL
                )
                operadora_bruta = match_operadora.group(1).strip() if match_operadora else "Não Identificada"

            # Normalização do nome da operadora
            operadora_bruta = unescape(operadora_bruta)
            op_upper = ''.join(char for char in unicodedata.normalize('NFD', operadora_bruta)
                               if unicodedata.category(char) != 'Mn').upper()
            if "ALFA" in op_upper:
                operadora = "ALFA"
            elif "BETA" in op_upper or "BETA" in op_upper:
                operadora = "Beta"
            elif "GAMA" in op_upper or "EPSILON" in op_upper or "NET" in op_upper:
                operadora = "Gama"
            elif "DELTA" in op_upper:
                operadora = "Delta"
            elif "ZETANET" in op_upper:
                operadora = "Zetanet Telecom"
            elif "ETACOM" in op_upper:
                operadora = "Etacom"
            else:
                operadora = operadora_bruta

            # Captura o valor do span de Portado (SIM / NÃO)
            match_portado = re.search(
                r'Portado:</span>\s*<span[^>]*class=["\']lead laranja["\'][^>]*>\s*([^<]+)</span>', 
                html, 
                re.IGNORECASE
            )
            portado = unescape(match_portado.group(1).strip()).capitalize() if match_portado else "Não"

            return {
                "Operadora_Atual": operadora,
                "Portado": portado
            }
        else:
            return {"Operadora_Atual": f"Erro HTTP {response.status_code}", "Portado": "Erro"}

    except Exception:
        return {"Operadora_Atual": "Erro Conexão/Timeout", "Portado": "Erro"}
def processar_base():
    print("A carregar ficheiro Excel...")
    df = pd.read_excel(ARQUIVO_ENTRADA)
    
    # A exportação preparada mantém K como "Tel" e insere L como "Telefone".
    # Escolher a coluna explicitamente evita consultar o número sem DDD.
    col_telefone = "Telefone"
    if len(df.columns) < 12 or df.columns[11] != col_telefone or df.columns[10] != "Tel":
        raise ValueError("Planilha inválida: esperadas Tel em K e Telefone em L")

    print(f"A processar coluna: '{col_telefone}'")

    resultados = []
    total = len(df)

    for index, row in df.iterrows():
        val_original = row[col_telefone]
        num_e164, status_estrutural, tipo, regiao = limpar_e_formatar_numero(val_original)
        
        operadora = "N/A"
        portado = "N/A"

        if status_estrutural == "Válido":
            info_web = consultar_consulta_operadora(num_e164)
            operadora = info_web["Operadora_Atual"]
            portado = info_web["Portado"]
            
            # Pequeno intervalo para evitar bloqueio por taxa de requisição
            time.sleep(0.1)

        resultados.append({
            "Telefone_Original": val_original,
            "Numero_Formatado": num_e164,
            "Status_Estrutural": status_estrutural,
            "Tipo": tipo,
            "Estado_Regiao": regiao,
            "Operadora_Atual": operadora,
            "Portado": portado
        })

        if (index + 1) % 50 == 0 or (index + 1) == total:
            print(f"Progresso: {index + 1}/{total} números processados...")
            Path("consulta_progresso.txt").write_text(str(index + 1), encoding="ascii")

    df_resultado = pd.DataFrame(resultados, columns=[
        "Telefone_Original", "Numero_Formatado", "Status_Estrutural", "Tipo",
        "Estado_Regiao", "Operadora_Atual", "Portado",
    ])
    df_resultado.to_excel(ARQUIVO_SAIDA, index=False)
    print(f"\nConcluído com sucesso! Ficheiro guardado em: {ARQUIVO_SAIDA}")

if __name__ == "__main__":
    processar_base()
