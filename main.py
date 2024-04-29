import pyodbc
import config
import pandas as pd


def extrair_dados_do_banco():
    # Substitua as informações de conexão conforme necessário
    server = config.server
    database = config.database
    username = config.username
    password = config.password

    # Construa a string de conexão
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Conecte-se ao banco de dados
    conexao = pyodbc.connect(conn_str)
    cursor = conexao.cursor()

    # Query SQL
    query = """
        SELECT 
            [CÓDIGO DA LOCALIDADE] + [CÓDIGO DO BAIRRO] AS "CHAVE_COD.LOC + COD.BAIRRO",
            [CÓDIGO DA LOCALIDADE] AS CODIGO_DA_LOCALIDADE,
            [CÓDIGO DO BAIRRO] AS CODIGO_DO_BAIRRO,
            BAIRRO,
            [ORDEM DE SERVIÇO] AS ORDEM_DE_SERVIÇO,
            [ÁREA DE TRABALHO] AS AREA_DE_TRABALHO,
            CIDADE,
            ESTADO,
            DATA
        FROM 
            [10.88.164.177].[DB_ALGAR_FIELD].[ALGR].[TB_ATIVIDADES] WITH (NOLOCK)
        WHERE 
            DATA_CARGA = (SELECT MAX(DATA_CARGA) FROM [10.88.164.177].[DB_ALGAR_FIELD].[ALGR].[TB_ATIVIDADES] WITH (NOLOCK))
            AND [STATUS DA ATIVIDADE] = 'PENDING'
            AND [ÁREA DE TRABALHO] = ''
            AND [CÓDIGO DO BAIRRO] <> ''
    """

    # Execute a consulta SQL
    cursor.execute(query)

    # Busque todos os resultados
    dados = cursor.fetchall()

    # Feche a conexão com o banco de dados
    conexao.close()

    return dados


# Chamando a função para extrair os dados
dados_extraidos = extrair_dados_do_banco()

df = pd.DataFrame(dados_extraidos)


####################### API ####################################
