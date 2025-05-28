#!/usr/bin/env python3
"""
API Cloud REAL para executar testefinal.py via n8n Cloud
Com código completo do ContaHub e Google Sheets
"""
import os
import sys
import json
import time
import logging
import tempfile
import datetime
import requests
import gspread
from datetime import datetime, date, timedelta
from flask import Flask, request, jsonify
from functools import wraps
from google.oauth2.service_account import Credentials

# Configuração
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações de segurança
API_KEY = os.getenv('API_KEY', 'minha_chave_secreta_testefinal_2025')

# Configurações ContaHub
CONTAHUB_EMAIL = os.getenv('CONTAHUB_EMAIL', 'digao@3768')
CONTAHUB_SENHA = os.getenv('CONTAHUB_SENHA', 'Geladeira@001')
LOGIN_URL = "https://sp.contahub.com/rest/contahub.cmds.UsuarioCmd/login/17421701611337?emp=0"
API_URL = "https://apiv2.contahub.com"
QUERY_ENDPOINT = "/query"

# Configurações Google Sheets
SHEET_NAME = os.getenv('SHEET_NAME', 'Base_de_dados_CA_ordinario')
GOOGLE_CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS', '{}')

# Datas fixas
DEFAULT_FIXED_START_DATE = '2025-05-22'
DEFAULT_FIXED_END_DATE = '2025-05-27'

def require_api_key(f):
    """Decorator para exigir API key"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Authorization header missing or invalid'}), 401
        
        token = auth_header.split(' ')[1]
        if token != API_KEY:
            return jsonify({'error': 'Invalid API key'}), 401
        
        return f(*args, **kwargs)
    return decorated_function

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de health check"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'testefinal-cloud-api-real',
        'version': '2.0.0'
    })

@app.route('/test', methods=['GET'])
def test_endpoint():
    """Endpoint de teste público"""
    return jsonify({
        'status': 'ok',
        'message': 'API REAL funcionando perfeitamente!',
        'timestamp': datetime.now().isoformat(),
        'environment': 'cloud',
        'contahub_email': CONTAHUB_EMAIL,
        'sheet_name': SHEET_NAME
    })

def login_contahub():
    """Realiza login no ContaHub e retorna a sessão"""
    try:
        logger.info(f"Tentando login no ContaHub com email: {CONTAHUB_EMAIL}")
        
        session = requests.Session()
        
        payload = {
            "usr_email": CONTAHUB_EMAIL,
            "usr_senha": CONTAHUB_SENHA,
            "emp": 0
        }
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = session.post(LOGIN_URL, json=payload, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                logger.info(f"Login realizado com sucesso para {CONTAHUB_EMAIL}")
                return session
            else:
                logger.error(f"Falha no login: {data.get('message', 'Erro desconhecido')}")
                return None
        else:
            logger.error(f"Erro HTTP no login: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Erro durante login: {str(e)}")
        return None

def fetch_data_contahub(session, module_name, start_date, end_date):
    """Busca dados de um módulo específico no ContaHub"""
    try:
        query_base_url = f"{API_URL}{QUERY_ENDPOINT}"
        
        # Queries para cada módulo
        queries = {
            'analitico': f"""
                SELECT v.dia_semana, v.semana, v.vd, v.vd_mesadesc, v.vd_localizacao, 
                       v.itm, v.trn, v.trn_desc, v.prefixo, v.tipo, v.tipovenda, v.ano, v.mes,
                       v.vd_dtgerencial, v.usr_lancou, v.prd, v.prd_desc, v.grp_desc, v.loc_desc,
                       v.qtd, v.desconto, v.valorfinal, v.custo, v.itm_obs, v.comandaorigem, v.itemorigem
                FROM contahub_analitico v 
                WHERE v.vd_dtgerencial BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY v.vd_dtgerencial, v.vd, v.itm
            """,
            'periodo': f"""
                SELECT v.vd, v.dia_semana, v.semana, v.trn, v.dt_gerencial, v.tipovenda,
                       v.vd_mesadesc, v.vd_localizacao, v.usr_abriu, v.pessoas, v.qtd_itens,
                       v.vr_pagamentos, v.vr_produtos, v.vr_repique, v.vr_couvert, v.vr_desconto,
                       v.motivo, v.dt_contabil, v.ultimo_pedido, v.vd_cpf, v.nf_autorizada,
                       v.nf_chaveacesso, v.nf_dtcontabil, v.vd_dtcontabil
                FROM contahub_periodo v 
                WHERE v.dt_gerencial BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY v.dt_gerencial, v.vd
            """
        }
        
        if module_name not in queries:
            logger.error(f"Módulo {module_name} não suportado")
            return None
            
        query = queries[module_name]
        
        response = session.post(query_base_url, json={"query": query}, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success') and data.get('data'):
                records = data['data']
                logger.info(f"Módulo {module_name}: {len(records)} registros obtidos")
                return records
            else:
                logger.warning(f"Módulo {module_name}: Nenhum dado encontrado")
                return []
        else:
            logger.error(f"Erro HTTP ao buscar {module_name}: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Erro ao buscar dados do módulo {module_name}: {str(e)}")
        return None

def process_data_analitico(records):
    """Processa dados analíticos"""
    if not records:
        return []
        
    processed_data = []
    
    for record in records:
        try:
            # Extrair dados do registro
            processed_record = [
                record.get('dia_semana', ''),
                record.get('semana', ''),
                record.get('vd', ''),
                record.get('vd_mesadesc', ''),
                record.get('vd_localizacao', ''),
                record.get('itm', ''),
                record.get('trn', ''),
                record.get('trn_desc', ''),
                record.get('prefixo', ''),
                record.get('tipo', ''),
                record.get('tipovenda', ''),
                record.get('ano', ''),
                record.get('mes', ''),
                record.get('vd_dtgerencial', ''),
                record.get('usr_lancou', ''),
                record.get('prd', ''),
                record.get('prd_desc', ''),
                record.get('grp_desc', ''),
                record.get('loc_desc', ''),
                float(record.get('qtd', 0) or 0),
                float(record.get('desconto', 0) or 0),
                float(record.get('valorfinal', 0) or 0),
                float(record.get('custo', 0) or 0),
                record.get('itm_obs', ''),
                record.get('comandaorigem', ''),
                record.get('itemorigem', '')
            ]
            processed_data.append(processed_record)
            
        except Exception as e:
            logger.error(f"Erro ao processar registro analítico: {str(e)}")
            continue
            
    return processed_data

def get_google_sheets_client():
    """Configura e retorna cliente Google Sheets"""
    try:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Parse credentials from environment variable
        credentials_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        creds = Credentials.from_service_account_info(credentials_dict, scopes=scope)
        client = gspread.authorize(creds)
        
        return client
        
    except Exception as e:
        logger.error(f"Erro ao configurar cliente Google Sheets: {str(e)}")
        return None

def append_to_google_sheets(worksheet_name, data):
    """Adiciona dados ao Google Sheets"""
    try:
        client = get_google_sheets_client()
        if not client:
            return None
            
        sheet = client.open(SHEET_NAME)
        worksheet = sheet.worksheet(worksheet_name)
        
        # Adicionar dados
        if data:
            worksheet.append_rows(data)
            start_row = len(worksheet.get_all_values()) - len(data) + 1
            end_row = len(worksheet.get_all_values())
            
            logger.info(f"Dados adicionados ao Google Sheets: linhas {start_row} a {end_row}")
            return (start_row, end_row)
        
        return None
        
    except Exception as e:
        logger.error(f"Erro ao adicionar dados ao Google Sheets: {str(e)}")
        return None

@app.route('/execute-testefinal', methods=['POST'])
@require_api_key
def execute_testefinal():
    """Endpoint para executar testefinal.py REAL"""
    try:
        logger.info(f"🚀 Executando TesteFinal REAL às {datetime.now()}")
        
        # Resultado da execução
        result = execute_testefinal_real()
        
        if result['success']:
            logger.info("✅ TesteFinal REAL executado com sucesso")
            return jsonify({
                'status': 'success',
                'message': 'TesteFinal REAL executado com sucesso',
                'timestamp': datetime.now().isoformat(),
                'data': result.get('data', {}),
                'execution_time': datetime.now().isoformat()
            })
        else:
            error_msg = result.get('error', 'Erro desconhecido na execução')
            logger.error(f"❌ Erro na execução: {error_msg}")
            return jsonify({
                'status': 'error',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }), 500
            
    except Exception as e:
        error_msg = f"Erro inesperado: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return jsonify({
            'status': 'error',
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }), 500

def execute_testefinal_real():
    """
    Executa o código REAL do testefinal.py
    """
    try:
        logger.info("🔐 Fazendo login no ContaHub...")
        
        # 1. Login no ContaHub
        session = login_contahub()
        if not session:
            return {
                'success': False,
                'error': 'Falha ao fazer login no ContaHub'
            }
        
        logger.info("✅ Login no ContaHub realizado com sucesso")
        
        # 2. Definir período (usando datas fixas por enquanto)
        start_date = DEFAULT_FIXED_START_DATE
        end_date = DEFAULT_FIXED_END_DATE
        
        logger.info(f"📅 Período: {start_date} até {end_date}")
        
        # 3. Processar módulo analítico
        logger.info("📊 Buscando dados analíticos...")
        records_analitico = fetch_data_contahub(session, 'analitico', start_date, end_date)
        
        if records_analitico is None:
            return {
                'success': False,
                'error': 'Erro ao buscar dados analíticos'
            }
        
        if not records_analitico:
            logger.warning("⚠️ Nenhum dado analítico encontrado")
            total_records = 0
        else:
            # 4. Processar dados
            logger.info(f"⚙️ Processando {len(records_analitico)} registros analíticos...")
            processed_data = process_data_analitico(records_analitico)
            
            if processed_data:
                # 5. Enviar para Google Sheets
                logger.info("📝 Enviando dados para Google Sheets...")
                result_sheets = append_to_google_sheets('analitico', processed_data)
                
                if result_sheets:
                    logger.info("✅ Dados enviados com sucesso para Google Sheets")
                    total_records = len(processed_data)
                else:
                    logger.error("❌ Erro ao enviar dados para Google Sheets")
                    return {
                        'success': False,
                        'error': 'Erro ao enviar dados para Google Sheets'
                    }
            else:
                total_records = 0
        
        # 6. Retornar sucesso
        return {
            'success': True,
            'data': {
                'processed_items': total_records,
                'sheets_updated': total_records > 0,
                'execution_date': datetime.now().isoformat(),
                'period': f"{start_date} até {end_date}",
                'modules_processed': ['analitico']
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Erro na execução real: {str(e)}", exc_info=True)
        return {
            'success': False,
            'error': f"Erro na execução: {str(e)}"
        }

@app.route('/logs', methods=['GET'])
@require_api_key
def get_logs():
    """Endpoint para ver logs recentes"""
    try:
        return jsonify({
            'status': 'success',
            'message': 'Logs disponíveis no dashboard da plataforma cloud',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    # Configurações para cloud
    port = int(os.getenv('PORT', 5000))
    
    print(f"🚀 API Cloud REAL iniciando na porta {port}")
    print(f"📋 Endpoints disponíveis:")
    print(f"   GET  /health")
    print(f"   GET  /test")
    print(f"   POST /execute-testefinal")
    print(f"   GET  /logs")
    print(f"🔐 API Key: {API_KEY}")
    print(f"📧 ContaHub Email: {CONTAHUB_EMAIL}")
    print(f"📊 Google Sheets: {SHEET_NAME}")
    print(f"🌐 Ambiente: Cloud REAL")
    
    app.run(host='0.0.0.0', port=port, debug=False) 