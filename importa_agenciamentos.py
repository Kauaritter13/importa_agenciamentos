import mysql.connector
import logging
import http.client
import json
from datetime import datetime
import urllib.parse
import base64
import requests

# Configuração do Logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"migration_log_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)

logging.info('Iniciando o script de migração de dados.')

try:
    # Conexão com o banco 'urban04'
    urban_conn = mysql.connector.connect(
        host='mysql.urban.imb.br',
        user='urban04',
        password='LrD84Bd0AlT32',
        database='urban04'
    )
    logging.info('Conectado ao banco de dados urban04 com sucesso.')

    # Conexão com o banco 'railway'
    railway_conn = mysql.connector.connect(
        host='viaduct.proxy.rlwy.net', 
        port=56119,
        user='root',
        password='seuZHHEcCeCAQuOSwjJcXOhtQfzCafbm',
        database='railway'
    )
    logging.info('Conectado ao banco de dados railway com sucesso.')

    urban_cursor = urban_conn.cursor()
    railway_cursor = railway_conn.cursor()

    # Limpar a tabela 'agenciamentos'
    logging.info('Limpando a tabela agenciamentos...')
    try:
        railway_cursor.execute("TRUNCATE TABLE agenciamentos;")
        logging.info('Tabela agenciamentos limpa com sucesso.')
    except mysql.connector.Error as err:
        logging.error(f'Erro ao limpar a tabela agenciamentos: {err}')
        raise

    # Buscar dados dos corretores
    logging.info('Buscando dados dos corretores...')
    urban_cursor.execute("""
        SELECT c.email, c.nome, c.codigo, c.abreviado
        FROM tb_corretores_vista c
        WHERE c.status = '1';
    """)
    corretores = urban_cursor.fetchall()
    logging.info(f'{len(corretores)} corretores encontrados.')

    # Processamento dos corretores
    for corretor in corretores:
        codigo_corretor = corretor[2]  # Código do corretor
        logging.info(f'Buscando imóveis para o corretor {corretor[1]} (Código: {codigo_corretor})')

        # Loop de paginação
        pagina_atual = 1
        while True:
            # Configurações da requisição
            conn = http.client.HTTPSConnection("urbanpoa20097-rest.vistahost.com.br")
            payload = ''
            headers = {
                'Accept': 'application/json'
            }

            params = {
                'key': '0000ac34876dec13413a9271aa6ca141',
                'showtotal': '1',
                'showSuspended': '1',
                'showInternal': '1',
                'pesquisa': json.dumps({
                    "fields": ["Dormitorios", "Status", "DataLiberacao", "DataCadastro", "Codigo", "Categoria", "Bairro", "Cidade", "ValorVenda", "TemPlaca"],
                    "filter": {
                        "CodigoCorretor": codigo_corretor  # Usando o código do corretor atual
                    },
                    "order": {
                        "DataCadastro": "asc"
                    },
                    "paginacao": {
                        "pagina": pagina_atual,
                        "quantidade": 50
                    }
                })
            }

            # Codifica os parâmetros de consulta
            query_string = urllib.parse.urlencode(params)

            try:
                # Faz a requisição GET
                conn.request("GET", f"/imoveis/listar?{query_string}", payload, headers)
                res = conn.getresponse()
                data = res.read()
                conn.close()
                data_str = data.decode("utf-8")
                if res.status != 200:
                    logging.error(f'Erro ao chamar a API para o corretor {corretor[1]}: {res.status} - {data_str}')
                    break

                # Log da resposta da API
                imoveis_response = json.loads(data_str)

                # Verifica se há imóveis
                if 'total' not in imoveis_response or not imoveis_response['total']:
                    logging.info(f'Nenhum imóvel encontrado para o corretor {corretor[1]} na página {pagina_atual}.')
                    break

                # Inserindo dados na tabela 'agenciamentos'
                for imovel_id, imovel in imoveis_response.items():
                    if imovel_id in ['total', 'paginas', 'pagina', 'quantidade']:
                        continue  # Ignora os metadados
                    try:
                        # Converte a data para o formato desejado, se necessário
                        data_cadastro = imovel['DataCadastro']  # Assumindo que a data está no formato 'YYYY-MM-DD'
                        data_liberacao = imovel['DataLiberacao']
                        valor_venda = imovel['ValorVenda']
                        if not data_liberacao or data_liberacao == '0000-00-00':
                            data_liberacao = None
                        if not valor_venda:
                            valor_venda = None
                        # Processa o campo 'TemPlaca'
                        placa = 1 if imovel.get('TemPlaca') == 'Sim' else 0

                        railway_cursor.execute("""
                            INSERT INTO agenciamentos (codigo_imovel, email_corretor, categoria, bairro, dormitorios, cidade, status, valor, data_cadastro, data_liberacao, placa)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """, (imovel['Codigo'], corretor[0], imovel['Categoria'], imovel['Bairro'], imovel['Dormitorios'], imovel['Cidade'], imovel['Status'], valor_venda, data_cadastro, data_liberacao, placa))
                        logging.info(f'Dados inseridos para o imóvel {imovel["Codigo"]} e corretor {corretor[1]}.')
                    except mysql.connector.Error as err:
                        logging.error(f'Erro ao inserir dados para o imóvel {imovel["Codigo"]}: {err}')

                # Verifica o total de páginas
                total_paginas = imoveis_response.get('paginas', 1)
                logging.info(f'Total de páginas: {total_paginas}')
                
                # Se a página atual for a última, interrompe o loop
                if pagina_atual >= total_paginas:
                    break

                pagina_atual += 1

            except Exception as e:
                logging.error(f'Erro ao chamar a API para o corretor {corretor[1]}: {e}')
                break

    railway_conn.commit()
    logging.info('Inserções concluídas e commit realizado com sucesso.')

except mysql.connector.Error as err:
    logging.critical(f'Erro de conexão ou execução: {err}')

finally:
    # Fechar as conexões
    if urban_cursor: urban_cursor.close()
    if railway_cursor: railway_cursor.close()
    if urban_conn: urban_conn.close()
    if railway_conn: railway_conn.close()
    logging.info('Conexões com os bancos de dados fechadas.')
    logging.info('Script de migração finalizado.')

    # Converter o arquivo de log para Base64
    log_file_path = f"migration_log_{datetime.now().strftime('%Y%m%d')}.log"
    try:
        with open(log_file_path, "rb") as log_file:
            log_base64 = base64.b64encode(log_file.read()).decode('utf-8')
        
        # Enviar o log via WhatsApp
        host = "urbancompany.megaapi.com.br"
        instance_key = "comunicacao_urban"
        token = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIyOS8wNy8yMDI0IiwibmFtZSI6IlVyYmFuIENvbXBhbnkifQ.msPAj8cbZ5JJLREax8WTjNcz7i6xLfPBflp8Px64TIHT7ve6OLmLLRzVjW-0EfvGkaH9aqWFh5XyQcwkCHVBHw"
        url = f"https://{host}/rest/sendMessage/{instance_key}/mediaBase64"

        message_data = {
            "messageData": {
                "to": "120363206846386741@g.us",
                "base64": f"data:text/plain;base64,{log_base64}",
                "fileName": log_file_path,
                "type": "document",
                "caption": "Log de migração",
                "mimeType": "text/plain"
            }
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }

        response = requests.post(url, json=message_data, headers=headers)

        if response.status_code == 200:
            logging.info('Arquivo de log enviado com sucesso via WhatsApp.')
        else:
            logging.error(f'Falha ao enviar o arquivo de log via WhatsApp. Status: {response.status_code}, Resposta: {response.text}')

    except Exception as e:
        logging.error(f'Erro ao enviar o arquivo de log via WhatsApp: {e}')