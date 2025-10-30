import os
import pandas as pd
import unicodedata
from datetime import datetime
import mysql.connector

# =========================================================
# FUNÇÃO: REMOVER ACENTOS E LIMPAR TEXTO
# =========================================================
def remover_acentos(texto):
    if not isinstance(texto, str):
        return texto
    texto_normalizado = unicodedata.normalize("NFD", texto)
    texto_sem_acento = "".join(
        c for c in texto_normalizado if not unicodedata.combining(c)
    )
    return texto_sem_acento.strip()

# =========================================================
# CONFIGURAÇÕES GERAIS
# =========================================================
CAMINHO_ARQUIVO = r"C:\Users\Jonas\Desktop\meu_airflow\dados_tcc\brutos\sigesguarda\2025-06-25_sigesguarda_-_Base_de_Dados.csv"
CAMINHO_SAIDA = r"C:\Users\Jonas\Desktop\meu_airflow\dados_tcc\transformados\sigesguarda\ocorrencias_transformado.csv"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    
    # Você já corrigiu isso, mas garanta que sua senha está aqui
    "password": "jonas", # Troque se necessário
    
    "database": "tcc_transito",
    "use_unicode": True,
    "charset": "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
    "autocommit": True
}

# =========================================================
# ETAPA 1 - LEITURA DO ARQUIVO
# =========================================================
print("=== Etapa 1: Leitura do arquivo CSV ===")

try:
    # Separador ',' está correto
    df = pd.read_csv(CAMINHO_ARQUIVO, encoding='utf-8', sep=',', low_memory=False)
    
    print(f"Arquivo '{os.path.basename(CAMINHO_ARQUIVO)}' lido com sucesso!")
except Exception as e:
    print(f"ERRO ao ler o arquivo: {e}")
    exit()

# =========================================================
# ETAPA 2 - TRATAMENTO DE DATA E HORA
# =========================================================
print("\n=== Etapa 2: Transformação de Data e Hora ===")

coluna_data = None
for c in df.columns:
    if "DATAHORA" in c.upper():
        coluna_data = c
        break
    if "DATA" in c.upper():
        coluna_data = c
        break

if not coluna_data:
    raise ValueError("Nenhuma coluna de data/hora encontrada no CSV.")

try:
    # --- CORREÇÃO 1 APLICADA AQUI ---
    # Adicionamos dayfirst=True para tratar o formato DD/MM/AAAA corretamente
    df["OCORRENCIA_TIMESTAMP"] = pd.to_datetime(df[coluna_data], errors="coerce", dayfirst=True)
except Exception:
    # Se falhar, tenta o formato específico
    df["OCORRENCIA_TIMESTAMP"] = pd.to_datetime(df[coluna_data], errors="coerce", format="%Y-%m-%d %H:%M:%S")

# Remove linhas onde a data não pôde ser convertida
df.dropna(subset=["OCORRENCIA_TIMESTAMP"], inplace=True)

df["ANO"] = df["OCORRENCIA_TIMESTAMP"].dt.year
df["MES"] = df["OCORRENCIA_TIMESTAMP"].dt.month
df["DIA"] = df["OCORRENCIA_TIMESTAMP"].dt.day
df["HORA"] = df["OCORRENCIA_TIMESTAMP"].dt.hour
df["MINUTO"] = df["OCORRENCIA_TIMESTAMP"].dt.minute
df["NOME_DIA_SEMANA"] = df["OCORRENCIA_TIMESTAMP"].dt.day_name(locale="pt_BR")
df["DIA_DA_SEMANA_NUM"] = df["OCORRENCIA_TIMESTAMP"].dt.dayofweek

print("Transformação de data e hora concluída.")

# =========================================================
# ETAPA 3 - SELEÇÃO E REORDENAÇÃO DE COLUNAS
# =========================================================
print("\n=== Etapa 3: Selecionando e reordenando colunas ===")

colunas_desejadas = [
    "OCORRENCIA_TIMESTAMP", "ANO", "MES", "DIA", "HORA", "MINUTO",
    "NOME_DIA_SEMANA", "DIA_DA_SEMANA_NUM",
    "ATENDIMENTO_BAIRRO_NOME", "LOGRADOURO_NOME", "REGIONAL_FATO_NOME",
    "NATUREZA1_DESCRICAO", "SUBCATEGORIA1_DESCRICAO", "EQUIPAMENTO_URBANO_NOME",
    "ORIGEM_CHAMADO_DESCRICAO", "OCORRENCIA_CODIGO", "NUMERO_PROTOCOLO_156"
]

colunas_existentes = [c for c in colunas_desejadas if c in df.columns]

if not colunas_existentes:
    print("ERRO: Nenhuma das colunas desejadas foi encontrada no CSV.")
    print("Colunas encontradas:", df.columns.tolist())
    exit()

df_final = df[colunas_existentes].copy()

# =========================================================
# ETAPA 4 - LIMPEZA DE TEXTO
# =========================================================
print("\n=== Etapa 4: Limpando e normalizando textos ===")

for col in df_final.select_dtypes(include=["object"]).columns:
    df_final[col] = df_final[col].fillna('').astype(str).apply(remover_acentos)

print("Acentos e espaços removidos com sucesso.")

# =========================================================
# ETAPA 5 - INFORMAÇÕES
# =========================================================
print("\n=== Etapa 5: Informações do DataFrame Final ===")
print(df_final.info())

# =========================================================
# ETAPA 6 - SALVAR CSV TRANSFORMADO
# =========================================================
print(f"\n=== Etapa 6: Salvando CSV em '{CAMINHO_SAIDA}' ===")

os.makedirs(os.path.dirname(CAMINHO_SAIDA), exist_ok=True)
df_final.to_csv(CAMINHO_SAIDA, encoding="utf-8", index=False)
print("Arquivo 'ocorrencias_transformado.csv' salvo com sucesso!")

# =========================================================
# ETAPA 7 - CARREGAR NO BANCO DE DADOS
# =========================================================
print("\n=== Etapa 7: Carregando dados no MySQL ===")

# --- CORREÇÃO 2 APLICADA AQUI ---
# Esta é a solução definitiva para o erro 'nan'.
# 1. Converte tudo para 'object' (forçando 'np.nan' a virar um objeto)
# 2. Substitui todos os nulos (nan, NaT, None) por 'None' do Python
df_final_sql = df_final.astype(object).where(pd.notnull(df_final), None)

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Conexão com MySQL estabelecida.")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ocorrencias (
            id INT AUTO_INCREMENT PRIMARY KEY,
            OCORRENCIA_TIMESTAMP DATETIME,
            ANO INT,
            MES INT,
            DIA INT,
            HORA INT,
            MINUTO INT,
            NOME_DIA_SEMANA VARCHAR(30),
            DIA_DA_SEMANA_NUM INT,
            ATENDIMENTO_BAIRRO_NOME VARCHAR(120),
            LOGRADOURO_NOME VARCHAR(180),
            REGIONAL_FATO_NOME VARCHAR(120),
            NATUREZA1_DESCRICAO VARCHAR(150),
            SUBCATEGORIA1_DESCRICAO VARCHAR(150),
            EQUIPAMENTO_URBANO_NOME VARCHAR(120),
            ORIGEM_CHAMADO_DESCRICAO VARCHAR(120),
            OCORRENCIA_CODIGO VARCHAR(60),
            NUMERO_PROTOCOLO_156 VARCHAR(60)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """)
    print("Tabela 'ocorrencias' verificada/criada.")

    conn.commit()

    insert_sql = f"""
        INSERT INTO ocorrencias ({', '.join(colunas_existentes)})
        VALUES ({', '.join(['%s'] * len(colunas_existentes))})
    """

    print("Iniciando inserção dos dados (executemany)...")
    # Usa a versão tratada para SQL (df_final_sql)
    cursor.executemany(insert_sql, df_final_sql.values.tolist())
    print("Dados enviados. Aguardando commit...")
    conn.commit()
    
    print(f"Dados carregados com sucesso no MySQL! {len(df_final_sql)} linhas inseridas.")

except mysql.connector.Error as err:
    print(f"ERRO ao carregar no MySQL: {err}")

finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("Conexão com o banco encerrada.")