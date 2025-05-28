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
        
        # Headers mais completos para parecer com um browser real
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"'
        })
        
        payload = {
            "usr_email": CONTAHUB_EMAIL,
            "usr_senha": CONTAHUB_SENHA,
            "emp": 0
        }
        
        logger.info(f"Fazendo POST para: {LOGIN_URL}")
        logger.info(f"Payload email: {payload['usr_email']}")
        logger.info(f"Payload senha: {'*' * len(payload['usr_senha'])}")
        
        # Primeiro, visitar a página principal para estabelecer sessão
        try:
            logger.info("Fazendo GET inicial para estabelecer sessão...")
            main_page = session.get('https://sp.contahub.com/', timeout=30)
            logger.info(f"GET principal - Status: {main_page.status_code}")
            logger.info(f"Cookies após GET principal: {dict(session.cookies)}")
            
            # Aguardar um pouco para simular comportamento humano
            time.sleep(2)
            
        except Exception as e:
            logger.warning(f"Erro no GET inicial (continuando): {str(e)}")
        
        # Tentar login
        headers_login = {
            'Content-Type': 'application/json',
            'Origin': 'https://sp.contahub.com',
            'Referer': 'https://sp.contahub.com/',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        response = session.post(LOGIN_URL, json=payload, headers=headers_login, timeout=60)
        
        logger.info(f"Status code: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        logger.info(f"Response cookies: {dict(session.cookies)}")
        logger.info(f"Response text (primeiros 1000 chars): {response.text[:1000]}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                logger.info(f"Response JSON completo: {data}")
                
                if data.get('success'):
                    logger.info(f"Login realizado com sucesso para {CONTAHUB_EMAIL}")
                    # Testar se a sessão realmente funciona fazendo uma requisição de teste
                    test_url = f"{API_URL}/test"
                    try:
                        test_response = session.get(test_url, timeout=30)
                        logger.info(f"Teste de sessão - Status: {test_response.status_code}")
                    except Exception as e:
                        logger.warning(f"Erro no teste de sessão: {str(e)}")
                    
                    return session
                else:
                    error_msg = data.get('message', 'Erro desconhecido')
                    logger.error(f"Falha no login - Resposta: {error_msg}")
                    logger.error(f"Data completo: {data}")
                    return None
                    
            except json.JSONDecodeError as e:
                logger.error(f"Erro ao decodificar JSON: {str(e)}")
                logger.error(f"Response raw: {response.text}")
                return None
                
        elif response.status_code == 429:
            logger.error("Rate limit atingido - muitas tentativas de login")
            return None
        elif response.status_code == 403:
            logger.error("Acesso negado - possível bloqueio de IP")
            return None
        else:
            logger.error(f"Erro HTTP no login: {response.status_code}")
            logger.error(f"Response headers: {dict(response.headers)}")
            logger.error(f"Response text: {response.text}")
            return None
            
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout durante login: {str(e)}")
        return None
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Erro de conexão durante login: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Erro durante login: {str(e)}", exc_info=True)
        return None

def login_contahub_alternative():
    """Método alternativo de login tentando replicar exatamente o testefinal.py local"""
    try:
        logger.info(f"Tentando login alternativo no ContaHub com email: {CONTAHUB_EMAIL}")
        
        session = requests.Session()
        
        # Headers simples como no testefinal.py local
        session.headers.update({
            'Content-Type': 'application/json'
        })
        
        payload = {
            "usr_email": CONTAHUB_EMAIL,
            "usr_senha": CONTAHUB_SENHA,
            "emp": 0
        }
        
        logger.info(f"Login alternativo - POST para: {LOGIN_URL}")
        
        # Login direto sem GET inicial
        response = session.post(LOGIN_URL, json=payload, timeout=30)
        
        logger.info(f"Login alternativo - Status: {response.status_code}")
        logger.info(f"Login alternativo - Response: {response.text[:500]}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                logger.info(f"Login alternativo - JSON: {data}")
                
                if data.get('success'):
                    logger.info(f"Login alternativo realizado com sucesso para {CONTAHUB_EMAIL}")
                    return session
                else:
                    logger.error(f"Login alternativo falhou: {data.get('message', 'Erro desconhecido')}")
                    return None
                    
            except json.JSONDecodeError as e:
                logger.error(f"Login alternativo - Erro JSON: {str(e)}")
                return None
        else:
            logger.error(f"Login alternativo - Erro HTTP: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Erro no login alternativo: {str(e)}", exc_info=True)
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
        
        # 1. Tentar login principal
        session = login_contahub()
        
        # 2. Se falhar, tentar método alternativo
        if not session:
            logger.info("🔄 Tentando método de login alternativo...")
            session = login_contahub_alternative()
        
        if not session:
            error_msg = 'Falha ao fazer login no ContaHub - tentados ambos os métodos'
            logger.error(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg
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

@app.route('/debug-login', methods=['GET'])
def debug_login():
    """Endpoint para debug do login ContaHub"""
    try:
        logger.info("🔍 Testando login ContaHub...")
        debug_info = {
            'timestamp': datetime.now().isoformat(),
            'environment': 'cloud',
            'contahub_email': CONTAHUB_EMAIL,
            'login_url': LOGIN_URL,
            'tests': {}
        }
        
        # Teste 1: Método principal
        logger.info("🔍 Teste 1: Método principal")
        session1 = login_contahub()
        debug_info['tests']['method_main'] = {
            'success': session1 is not None,
            'message': 'Login principal funcionou' if session1 else 'Login principal falhou'
        }
        
        # Teste 2: Método alternativo
        logger.info("🔍 Teste 2: Método alternativo")
        session2 = login_contahub_alternative()
        debug_info['tests']['method_alternative'] = {
            'success': session2 is not None,
            'message': 'Login alternativo funcionou' if session2 else 'Login alternativo falhou'
        }
        
        # Teste 3: Conectividade básica
        logger.info("🔍 Teste 3: Conectividade básica")
        try:
            test_response = requests.get('https://sp.contahub.com/', timeout=10)
            connectivity_test = {
                'success': test_response.status_code == 200,
                'status_code': test_response.status_code,
                'message': f'Conectividade OK (status {test_response.status_code})' if test_response.status_code == 200 else f'Problema de conectividade (status {test_response.status_code})'
            }
        except Exception as e:
            connectivity_test = {
                'success': False,
                'error': str(e),
                'message': f'Erro de conectividade: {str(e)}'
            }
        
        debug_info['tests']['connectivity'] = connectivity_test
        
        # Resultado final
        any_success = any(test.get('success', False) for test in debug_info['tests'].values())
        
        if any_success:
            debug_info['overall_status'] = 'success'
            debug_info['message'] = 'Pelo menos um método de login funcionou!'
            return jsonify(debug_info)
        else:
            debug_info['overall_status'] = 'error'
            debug_info['message'] = 'Todos os métodos de login falharam'
            return jsonify(debug_info), 500
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/debug-env', methods=['GET'])
def debug_env():
    """Endpoint para verificar variáveis de ambiente"""
    return jsonify({
        'contahub_email': CONTAHUB_EMAIL,
        'contahub_senha': CONTAHUB_SENHA[:3] + '***',  # Só os 3 primeiros caracteres
        'sheet_name': SHEET_NAME,
        'google_credentials_loaded': bool(GOOGLE_CREDENTIALS_JSON != '{}'),
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    # Configurações para cloud
    port = int(os.getenv('PORT', 5000))
    
    print(f"🚀 API Cloud REAL iniciando na porta {port}")
    print(f"📋 Endpoints disponíveis:")
    print(f"   GET  /health")
    print(f"   GET  /test")
    print(f"   POST /execute-testefinal")
    print(f"   GET  /logs")
    print(f"   GET  /debug-login")
    print(f"   GET  /debug-env")
    print(f"🔐 API Key: {API_KEY}")
    print(f"📧 ContaHub Email: {CONTAHUB_EMAIL}")
    print(f"📊 Google Sheets: {SHEET_NAME}")
    print(f"🌐 Ambiente: Cloud REAL")
    
    app.run(host='0.0.0.0', port=port, debug=False) 
