import pandas as pd
import geopandas as gpd
import os
import sys
import mysql.connector  # <-- 1. ADICIONADO IMPORT

# =============================================================================
# SEÇÃO DE CONFIGURAÇÃO
# =============================================================================

# --- INÍCIO DA SEÇÃO DE CONFIGURAÇÃO INTELIGENTE ---
# ... (toda a sua configuração de pastas fica igual) ...
if 'AIRFLOW_HOME' in os.environ:
    print("Executando em ambiente Airflow/Docker.")
    PASTA_RAIZ_DADOS = "/opt/airflow/data"
else:
    print("Executando em ambiente local (Windows).")
    PASTA_RAIZ_DADOS = r"C:\Users\Jonas\Desktop\meu_airflow\dados_tcc"

PASTA_BRUTOS_IPPUC = os.path.join(PASTA_RAIZ_DADOS, "brutos", "ippuc", "SIST_VIARIO_CLASSIFICADO_SIRGAS")
PASTA_SAIDA_TRANSFORMADOS = os.path.join(PASTA_RAIZ_DADOS, "transformados", "ippuc")
NOME_ARQUIVO_ENTRADA_SHP = "SIST_VIARIO_CLASSIFICADO.shp"
NOME_ARQUIVO_SAIDA = "sistema_viario_transformado.csv"
# --- FIM DA SEÇÃO DE CONFIGURAÇÃO INTELIGENTE ---


# --- 2. ADICIONADO DB_CONFIG ---
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "jonas",  # <-- LEMBRE-SE DE COLOCAR SUA SENHA
    "database": "tcc_transito",
    "use_unicode": True,
    "charset": "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
    "autocommit": True
}
# ---------------------------------


# =============================================================================
# ETAPA 1: EXTRAÇÃO (Extract)
# =============================================================================
def extrair_dados_ippuc(caminho_completo_shp: str) -> gpd.GeoDataFrame | None:
    """Lê um arquivo Shapefile do IPPUC e retorna um GeoDataFrame."""
    try:
        gdf = gpd.read_file(caminho_completo_shp)
        print(f"Arquivo '{os.path.basename(caminho_completo_shp)}' lido com sucesso!")
        return gdf
    except Exception as e:
        print(f"Ocorreu um erro inesperado ao ler o arquivo Shapefile: {e}")
        return None

# =============================================================================
# ETAPA 2: TRANSFORMAÇÃO (Transform)
# =============================================================================
def transformar_dados_ippuc(gdf_bruto: gpd.GeoDataFrame) -> pd.DataFrame:
    """Aplica as transformações de limpeza e enriquecimento nos dados do IPPUC."""
    gdf_transformado = gdf_bruto.copy()
    print("\n--- Iniciando transformação dos dados geográficos ---")
    
    colunas_selecionadas = ['NMVIA', 'SIST_VIARI', 'SIST_VIA_1', 'geometry']
    gdf_transformado = gdf_transformado[colunas_selecionadas]

    mapa_renomeacao = {
        'NMVIA': 'nome_via', 
        'SIST_VIARI': 'sistema_viario', 
        'SIST_VIA_1': 'hierarquia_via'
    }
    gdf_transformado = gdf_transformado.rename(columns=mapa_renomeacao)

    # Converte a coluna de geometria para o formato WKT (texto)
    # Isso é necessário tanto para o CSV quanto para o banco (tipo TEXT)
    gdf_transformado['geometry'] = gdf_transformado['geometry'].apply(lambda geom: geom.wkt)
    print("Coluna de geometria convertida para WKT.")

    return gdf_transformado

# =============================================================================
# ETAPA 3_A: CARREGAMENTO (Load - para CSV)
# =============================================================================
def salvar_csv_transformado(df_para_salvar: pd.DataFrame, caminho_saida: str, nome_arquivo: str):
    """Salva o DataFrame transformado em um arquivo CSV."""
    os.makedirs(caminho_saida, exist_ok=True)
    caminho_completo = os.path.join(caminho_saida, nome_arquivo)
    
    print(f"\n--- Iniciando salvamento para o arquivo CSV em '{caminho_completo}' ---")
    try:
        df_para_salvar.to_csv(caminho_completo, index=False, sep=';', encoding='utf-8-sig')
        print(f"Arquivo '{nome_arquivo}' salvo com sucesso!")
    except Exception as e:
        print(f"ERRO ao salvar o arquivo CSV: {e}")
        sys.exit(1)

# =============================================================================
# ETAPA 3_B: CARREGAMENTO (Load - para Banco de Dados)
# (Lógica "transplantada" do outro script)
# =============================================================================
def carregar_banco_dados(df_para_carregar: pd.DataFrame):
    """Carrega o DataFrame transformado na tabela 'sistema_viario' do MySQL."""
    print("\n--- Iniciando carregamento para o Banco de Dados MySQL ---")

    # Prepara o DataFrame para SQL (converte 'nan' para 'None')
    df_sql = df_para_carregar.astype(object).where(pd.notnull(df_para_carregar), None)
    
    # Pega os nomes das colunas do DataFrame
    nomes_colunas = list(df_sql.columns)
    
    # 4. AJUSTADO O CREATE TABLE
    # (Usamos LONGTEXT para a geometria em WKT, que pode ser bem longa)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sistema_viario (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nome_via VARCHAR(255),
        sistema_viario VARCHAR(100),
        hierarquia_via VARCHAR(100),
        geometry LONGTEXT
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """
    
    # 5. AJUSTADO O INSERT
    # Cria os placeholders (%s) dinamicamente
    placeholders = ', '.join(['%s'] * len(nomes_colunas))
    insert_sql = f"INSERT INTO sistema_viario ({', '.join(nomes_colunas)}) VALUES ({placeholders})"

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Conexão com MySQL estabelecida.")
        
        cursor.execute(create_table_sql)
        print("Tabela 'sistema_viario' verificada/criada.")
        conn.commit()

        print("Iniciando inserção dos dados (executemany)...")
        cursor.executemany(insert_sql, df_sql.values.tolist())
        print("Dados enviados. Aguardando commit...")
        conn.commit()
        
        print(f"Dados carregados com sucesso no MySQL! {len(df_sql)} linhas inseridas.")

    except mysql.connector.Error as err:
        print(f"ERRO ao carregar no MySQL: {err}")
        # Lembrete: se o erro for 1146 (Table doesn't exist), algo deu errado no CREATE.
        # Se for 1054 (Unknown column), os nomes das colunas estão errados.

    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexão com o banco encerrada.")

# =============================================================================
# BLOCO DE EXECUÇÃO PRINCIPAL
# =============================================================================
if __name__ == "__main__":
    caminho_shp_entrada = os.path.join(PASTA_BRUTOS_IPPUC, NOME_ARQUIVO_ENTRADA_SHP)
    
    gdf_bruto = extrair_dados_ippuc(caminho_shp_entrada)
    
    if gdf_bruto is not None and not gdf_bruto.empty:
        df_final = transformar_dados_ippuc(gdf_bruto)
        print("\n--- Informações do DataFrame Final ---")
        df_final.info()
        
        # 1. Salva no CSV (como já fazia)
        salvar_csv_transformado(
            df_para_salvar=df_final,
            caminho_saida=PASTA_SAIDA_TRANSFORMADOS,
            nome_arquivo=NOME_ARQUIVO_SAIDA
        )
        
        # 6. ADICIONADA CHAMADA PARA CARREGAR NO BANCO
        carregar_banco_dados(df_final)
        
    else:
        print("\nPipeline encerrado. Não foi possível extrair os dados.")