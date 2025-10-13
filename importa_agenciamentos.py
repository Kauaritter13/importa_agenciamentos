import os
import mysql.connector
from mysql.connector import pooling
import logging
import json
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple
import hashlib
import time
from urllib.parse import urlparse

# Configuração do Logger
log_level = os.getenv('LOG_LEVEL', 'WARNING')  # Mudado de INFO para WARNING
log_to_file = os.getenv('LOG_TO_FILE', 'true').lower() == 'true'

handlers = [logging.StreamHandler()]
if log_to_file:
    handlers.append(logging.FileHandler(f"migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"))

logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=handlers
)

logger = logging.getLogger(__name__)

# Configurações de ambiente
class Config:
    # Database URLs
    SOURCE_DB_URL = os.getenv('SOURCE_DB_URL')
    TARGET_DB_URL = os.getenv('TARGET_DB_URL')

    # API Configuration
    VISTA_API_HOST = os.getenv('VISTA_API_HOST')
    VISTA_API_KEY = os.getenv('VISTA_API_KEY')

    # WhatsApp
    WHATSAPP_HOST = os.getenv('WHATSAPP_HOST')
    WHATSAPP_INSTANCE_KEY = os.getenv('WHATSAPP_INSTANCE_KEY')
    WHATSAPP_TOKEN = os.getenv('WHATSAPP_TOKEN')
    WHATSAPP_GROUP_ID = os.getenv('WHATSAPP_GROUP_ID')
    SEND_LOG_TO_WHATSAPP = os.getenv('SEND_LOG_TO_WHATSAPP', 'false').lower() == 'true'

    # Performance
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '10'))
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '30'))
    DB_POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '20'))
    DB_POOL_OVERFLOW = int(os.getenv('DB_POOL_OVERFLOW', '10'))
    CACHE_TTL = int(os.getenv('CACHE_TTL', '300'))  # Cache TTL em segundos

    @classmethod
    def validate(cls):
        """Valida se todas as variáveis obrigatórias estão configuradas"""
        required = [
            'SOURCE_DB_URL', 'TARGET_DB_URL', 'VISTA_API_HOST', 'VISTA_API_KEY'
        ]

        missing = []
        for var in required:
            if not getattr(cls, var):
                missing.append(var)

        if missing:
            raise ValueError(f"Variáveis de ambiente obrigatórias não configuradas: {', '.join(missing)}")

    @classmethod
    def parse_db_url(cls, url: str) -> dict:
        """Parse uma URL de banco MySQL para parâmetros de conexão"""
        parsed = urlparse(url)
        config = {
            'host': parsed.hostname,
            'port': parsed.port or 3306,
            'user': parsed.username,
            'password': parsed.password,
            'database': parsed.path.lstrip('/')
        }
        # Debug do parse
        logger.debug(f"Parsed URL {url}: {config}")
        return config

# Validar configuração
Config.validate()

# Parse das URLs de banco
source_config = Config.parse_db_url(Config.SOURCE_DB_URL)
target_config = Config.parse_db_url(Config.TARGET_DB_URL)

# Pool de conexões para banco de origem
source_pool = pooling.MySQLConnectionPool(
    pool_name="source_pool",
    pool_size=Config.DB_POOL_SIZE,
    pool_reset_session=True,
    **source_config
)

# Pool de conexões para banco de destino
target_pool = pooling.MySQLConnectionPool(
    pool_name="target_pool",
    pool_size=Config.DB_POOL_SIZE,
    pool_reset_session=True,
    **target_config
)

# Session HTTP com retry e connection pooling
def create_http_session() -> requests.Session:
    """Cria uma sessão HTTP otimizada com retry e pooling"""
    session = requests.Session()
    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504)
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=Config.MAX_WORKERS,
        pool_maxsize=Config.MAX_WORKERS * 2
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# Cache global para requisições
request_cache: Dict[str, Tuple[Any, float]] = {}

def get_cached_or_fetch(session: requests.Session, url: str, headers: dict) -> Optional[dict]:
    """Busca dados do cache ou faz requisição"""
    cache_key = hashlib.md5(url.encode()).hexdigest()

    # Verifica cache
    if cache_key in request_cache:
        data, timestamp = request_cache[cache_key]
        if time.time() - timestamp < Config.CACHE_TTL:
            return data

    try:
        response = session.get(url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
        if response.status_code == 200:
            data = response.json()
            request_cache[cache_key] = (data, time.time())
            return data
        else:
            logger.error(f"Erro na requisição: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Erro ao fazer requisição: {e}")
        return None

def create_imovel_hash(imovel: dict) -> str:
    """Cria um hash dos dados do imóvel para detectar mudanças"""
    relevant_fields = ['Categoria', 'Bairro', 'Dormitorios', 'Cidade', 'Status',
                      'ValorVenda', 'DataCadastro', 'DataLiberacao', 'TemPlaca', 'ExibirNoSite', 'AreaPrivativa', 'Latitude', 'Longitude']

    hash_data = {}
    for field in relevant_fields:
        if field in imovel:
            hash_data[field] = imovel[field]

    hash_string = json.dumps(hash_data, sort_keys=True)
    return hashlib.md5(hash_string.encode()).hexdigest()

def ensure_table_structure():
    """Garante que a tabela tenha a estrutura correta com índices"""
    conn = target_pool.get_connection()
    cursor = conn.cursor()

    try:
        # Criar tabela se não existir
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agenciamentos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                codigo_imovel VARCHAR(50) NOT NULL,
                email_corretor VARCHAR(255) NOT NULL,
                categoria VARCHAR(100),
                bairro VARCHAR(100),
                dormitorios VARCHAR(10),
                cidade VARCHAR(100),
                status VARCHAR(50),
                valor DECIMAL(15,2),
                data_cadastro DATE,
                data_liberacao DATE,
                placa TINYINT(1),
                exibir_site BOOLEAN,
                data_hash VARCHAR(32),
                UNIQUE KEY uk_imovel_corretor (codigo_imovel, email_corretor),
                INDEX idx_corretor (email_corretor),
                INDEX idx_status (status),
                INDEX idx_hash (data_hash)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # Adicionar coluna data_hash se não existir
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = 'agenciamentos'
            AND COLUMN_NAME = 'data_hash'
        """, (target_config['database'],))

        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                ALTER TABLE agenciamentos
                ADD COLUMN data_hash VARCHAR(32),
                ADD INDEX idx_hash (data_hash)
            """)
            logger.warning("Coluna data_hash adicionada à tabela")

        # Adicionar coluna exibir_site se não existir
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = 'agenciamentos'
            AND COLUMN_NAME = 'exibir_site'
        """, (target_config['database'],))

        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                ALTER TABLE agenciamentos
                ADD COLUMN exibir_site BOOLEAN
            """)
            logger.warning("Coluna exibir_site adicionada à tabela")

        # Adicionar coluna metragem se não existir
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = 'agenciamentos'
            AND COLUMN_NAME = 'metragem'
        """, (target_config['database'],))

        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                ALTER TABLE agenciamentos
                ADD COLUMN metragem FLOAT
            """)
            logger.warning("Coluna metragem adicionada à tabela")

        # Adicionar coluna latitude se não existir
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = 'agenciamentos'
            AND COLUMN_NAME = 'latitude'
        """, (target_config['database'],))

        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                ALTER TABLE agenciamentos
                ADD COLUMN latitude FLOAT
            """)
            logger.warning("Coluna latitude adicionada à tabela")

        # Adicionar coluna longitude se não existir
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = 'agenciamentos'
            AND COLUMN_NAME = 'longitude'
        """, (target_config['database'],))

        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                ALTER TABLE agenciamentos
                ADD COLUMN longitude FLOAT
            """)
            logger.warning("Coluna longitude adicionada à tabela")

        conn.commit()

    finally:
        cursor.close()
        conn.close()

def get_existing_records() -> Dict[str, str]:
    """Obtém hash dos registros existentes para comparação"""
    conn = target_pool.get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT CONCAT(TRIM(codigo_imovel), '|', TRIM(email_corretor)) as chave, data_hash
            FROM agenciamentos
            WHERE data_hash IS NOT NULL
        """)

        existing = {row[0]: row[1] for row in cursor.fetchall()}
        return existing

    finally:
        cursor.close()
        conn.close()

def process_corretor_batch(corretores_batch: List[tuple], session: requests.Session, existing_records: Dict[str, str]) -> Dict[str, Any]:
    """Processa um lote de corretores em paralelo"""
    stats = {
        'total_imoveis': 0,
        'inserted': 0,
        'updated': 0,
        'unchanged': 0,
        'errors': 0
    }

    all_records = []

    for corretor in corretores_batch:
        email_corretor = corretor[0]
        nome_corretor = corretor[1]
        codigo_corretor = corretor[2]

        pagina_atual = 1
        corretor_total = 0

        while True:
            # Monta a URL da requisição
            params = {
                'key': Config.VISTA_API_KEY,
                'showtotal': '1',
                'showSuspended': '1',
                'showInternal': '1',
                'pesquisa': json.dumps({
                    "fields": ["Dormitorios", "Status", "DataLiberacao", "DataCadastro",
                              "Codigo", "Categoria", "Bairro", "Cidade", "ValorVenda", "TemPlaca", "ExibirNoSite", "AreaPrivativa", "Latitude", "Longitude"],
                    "filter": {"CodigoCorretor": codigo_corretor},
                    "order": {"DataCadastro": "asc"},
                    "paginacao": {"pagina": pagina_atual, "quantidade": 50}
                })
            }

            url = f"https://{Config.VISTA_API_HOST}/imoveis/listar"
            headers = {'Accept': 'application/json'}

            # Requisição com cache
            full_url = f"{url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
            imoveis_response = get_cached_or_fetch(session, full_url, headers)

            if not imoveis_response or 'total' not in imoveis_response or not imoveis_response['total']:
                break

            # Processa imóveis
            for imovel_id, imovel in imoveis_response.items():
                if imovel_id in ['total', 'paginas', 'pagina', 'quantidade']:
                    continue

                try:
                    # Prepara dados
                    data_cadastro = imovel['DataCadastro']
                    data_liberacao = imovel['DataLiberacao']
                    valor_venda = imovel['ValorVenda']

                    if not data_liberacao or data_liberacao == '0000-00-00':
                        data_liberacao = None
                    if not valor_venda:
                        valor_venda = None

                    placa = 1 if imovel.get('TemPlaca') == 'Sim' else 0
                    exibir_site = True if imovel.get('ExibirNoSite') == 'Sim' else False

                    # Pega área privativa e converte para float
                    metragem = None
                    if imovel.get('AreaPrivativa'):
                        try:
                            metragem = float(imovel['AreaPrivativa'])
                        except (ValueError, TypeError):
                            metragem = None

                    # Pega latitude e longitude e converte para float
                    latitude = None
                    if imovel.get('Latitude'):
                        try:
                            latitude = float(imovel['Latitude'])
                        except (ValueError, TypeError):
                            latitude = None

                    longitude = None
                    if imovel.get('Longitude'):
                        try:
                            longitude = float(imovel['Longitude'])
                        except (ValueError, TypeError):
                            longitude = None

                    # Calcula hash para detectar mudanças
                    data_hash = create_imovel_hash(imovel)

                    # Limpa os campos chave para garantir consistência
                    codigo_imovel_clean = str(imovel['Codigo']).strip()
                    email_corretor_clean = str(email_corretor).strip()
                    record_key = f"{codigo_imovel_clean}|{email_corretor_clean}"

                    # Verifica se precisa atualizar
                    existing_hash = existing_records.get(record_key)

                    if existing_hash == data_hash:
                        stats['unchanged'] += 1
                        continue

                    # Adiciona à lista de records para batch insert/update
                    all_records.append((
                        codigo_imovel_clean, email_corretor_clean, imovel['Categoria'],
                        imovel['Bairro'], imovel['Dormitorios'], imovel['Cidade'],
                        imovel['Status'], valor_venda, data_cadastro,
                        data_liberacao, placa, exibir_site, metragem, latitude, longitude, data_hash
                    ))

                    corretor_total += 1

                except Exception as e:
                    logger.error(f"Erro processando imóvel {imovel.get('Codigo', 'unknown')}: {e}")
                    stats['errors'] += 1

            # Verifica próxima página
            total_paginas = imoveis_response.get('paginas', 1)
            if pagina_atual >= total_paginas:
                break

            pagina_atual += 1

        stats['total_imoveis'] += corretor_total

    # Batch upsert
    if all_records:
        inserted, updated = batch_upsert_records(all_records)
        stats['inserted'] = inserted
        stats['updated'] = updated

    return stats

def batch_upsert_records(records: List[tuple]) -> Tuple[int, int]:
    """Realiza batch upsert de registros"""
    if not records:
        return 0, 0

    conn = target_pool.get_connection()
    cursor = conn.cursor()

    try:
        # Primeiro, vamos verificar quantos registros já existem
        existing_check_query = """
            SELECT COUNT(*) FROM agenciamentos
            WHERE (codigo_imovel, email_corretor) IN (%s)
        """

        # Cria placeholders para a consulta
        record_keys = [(record[0], record[1]) for record in records]
        placeholders = ','.join(['(%s,%s)'] * len(record_keys))
        flat_keys = [item for pair in record_keys for item in pair]

        # Batch insert com ON DUPLICATE KEY UPDATE
        query = """
            INSERT INTO agenciamentos (
                codigo_imovel, email_corretor, categoria, bairro, dormitorios,
                cidade, status, valor, data_cadastro, data_liberacao, placa, exibir_site, metragem, latitude, longitude, data_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                categoria = VALUES(categoria),
                bairro = VALUES(bairro),
                dormitorios = VALUES(dormitorios),
                cidade = VALUES(cidade),
                status = VALUES(status),
                valor = VALUES(valor),
                data_cadastro = VALUES(data_cadastro),
                data_liberacao = VALUES(data_liberacao),
                placa = VALUES(placa),
                exibir_site = VALUES(exibir_site),
                metragem = VALUES(metragem),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                data_hash = VALUES(data_hash)
        """

        # Executa em lotes menores se necessário
        batch_size = Config.BATCH_SIZE
        total_inserted = 0
        total_updated = 0

        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]

            cursor.executemany(query, batch)

            # Calcula inserções vs atualizações
            affected = cursor.rowcount
            # No MySQL, ON DUPLICATE KEY UPDATE retorna 2 para update e 1 para insert
            # Estimativa: se affected > len(batch), houve updates
            if affected > len(batch):
                batch_updated = affected - len(batch)
                batch_inserted = len(batch) * 2 - affected
                total_updated += batch_updated
                total_inserted += batch_inserted
            else:
                total_inserted += affected

            conn.commit()

        return total_inserted, total_updated

    except Exception as e:
        logger.error(f"Erro no batch upsert: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def clean_old_records():
    """Remove registros de corretores que não estão mais ativos"""
    conn_source = source_pool.get_connection()
    conn_target = target_pool.get_connection()

    try:
        cursor_source = conn_source.cursor()
        cursor_target = conn_target.cursor()

        # Busca emails ativos
        cursor_source.execute("""
            SELECT DISTINCT email FROM tb_corretores_vista WHERE status = '1'
        """)
        active_emails = [row[0] for row in cursor_source.fetchall()]

        if active_emails:
            # Remove registros de corretores inativos
            placeholders = ','.join(['%s'] * len(active_emails))
            cursor_target.execute(f"""
                DELETE FROM agenciamentos
                WHERE email_corretor NOT IN ({placeholders})
            """, active_emails)

            deleted = cursor_target.rowcount
            if deleted > 0:
                logger.warning(f"Removidos {deleted} registros de corretores inativos")
                conn_target.commit()

        cursor_source.close()
        cursor_target.close()

    finally:
        conn_source.close()
        conn_target.close()

def send_log_to_whatsapp(log_file_path: str):
    """Envia log via WhatsApp se configurado"""
    if not Config.SEND_LOG_TO_WHATSAPP:
        return

    if not all([Config.WHATSAPP_HOST, Config.WHATSAPP_INSTANCE_KEY,
                Config.WHATSAPP_TOKEN, Config.WHATSAPP_GROUP_ID]):
        logger.warning("Configuração do WhatsApp incompleta, pulando envio de log")
        return

    try:
        with open(log_file_path, "rb") as log_file:
            log_base64 = base64.b64encode(log_file.read()).decode('utf-8')

        url = f"https://{Config.WHATSAPP_HOST}/rest/sendMessage/{Config.WHATSAPP_INSTANCE_KEY}/mediaBase64"

        message_data = {
            "messageData": {
                "to": Config.WHATSAPP_GROUP_ID,
                "base64": f"data:text/plain;base64,{log_base64}",
                "fileName": log_file_path,
                "type": "document",
                "caption": f"Log de migração - {datetime.now().strftime('%d/%m/%Y %H:%M')}",
                "mimeType": "text/plain"
            }
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {Config.WHATSAPP_TOKEN}"
        }

        response = requests.post(url, json=message_data, headers=headers, timeout=30)

        if response.status_code != 200:
            logger.error(f'Falha ao enviar log via WhatsApp: {response.status_code}')

    except Exception as e:
        logger.error(f'Erro ao enviar log via WhatsApp: {e}')

def main():
    """Função principal otimizada"""
    start_time = time.time()

    print('='*50)
    print('Iniciando migração otimizada de dados')
    print(f'Configuração: {Config.MAX_WORKERS} workers, batch size {Config.BATCH_SIZE}')
    print('='*50)

    try:
        # Garante estrutura da tabela
        ensure_table_structure()

        # Obtém registros existentes
        print("Carregando registros existentes...")
        existing_records = get_existing_records()
        print(f"Encontrados {len(existing_records)} registros existentes")

        # Busca corretores ativos
        conn_source = source_pool.get_connection()
        cursor_source = conn_source.cursor()

        cursor_source.execute("""
            SELECT c.email, c.nome, c.codigo, c.abreviado
            FROM tb_corretores_vista c
            WHERE c.status = '1'
            ORDER BY c.codigo;
        """)

        all_corretores = cursor_source.fetchall()
        cursor_source.close()
        conn_source.close()

        print(f"Encontrados {len(all_corretores)} corretores ativos")

        # Processa em paralelo
        total_stats = {
            'total_imoveis': 0,
            'inserted': 0,
            'updated': 0,
            'unchanged': 0,
            'errors': 0
        }

        # Cria sessão HTTP compartilhada
        session = create_http_session()

        # Divide corretores em batches para processamento paralelo
        batch_size = max(1, len(all_corretores) // Config.MAX_WORKERS)
        corretor_batches = [all_corretores[i:i+batch_size]
                           for i in range(0, len(all_corretores), batch_size)]

        with ThreadPoolExecutor(max_workers=min(Config.MAX_WORKERS, len(corretor_batches))) as executor:
            futures = []

            for batch in corretor_batches:
                future = executor.submit(process_corretor_batch, batch, session, existing_records)
                futures.append(future)

            # Processa resultados conforme completam
            for future in as_completed(futures):
                try:
                    stats = future.result()
                    for key in total_stats:
                        total_stats[key] += stats.get(key, 0)

                except Exception as e:
                    logger.error(f"Erro no batch: {e}")

        # Limpa registros antigos
        clean_old_records()

        # Estatísticas finais
        elapsed_time = time.time() - start_time
        print('='*50)
        print(f"Migração concluída em {elapsed_time:.2f} segundos ({elapsed_time/60:.2f} minutos)")
        print(f"Total de imóveis processados: {total_stats['total_imoveis']}")
        print(f"Registros inseridos: {total_stats['inserted']}")
        print(f"Registros atualizados: {total_stats['updated']}")
        print(f"Registros inalterados: {total_stats['unchanged']}")
        print(f"Erros: {total_stats['errors']}")
        print(f"Performance: {total_stats['total_imoveis']/elapsed_time:.2f} imóveis/segundo")
        print('='*50)

    except Exception as e:
        logger.critical(f'Erro crítico na execução: {e}', exc_info=True)
        raise

    finally:
        # Envia log se configurado
        if log_to_file:
            log_file = f"migration_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            send_log_to_whatsapp(log_file)

if __name__ == "__main__":
    main() 
