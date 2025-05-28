#!/usr/bin/env python3
"""
API Cloud com PROXY brasileiro para ContaHub
Versão com proxy para contornar bloqueio de IP
"""
import os
import sys
import json
import time
import logging
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

# Lista de proxies brasileiros gratuitos (rotativos)
BRAZILIAN_PROXIES = [
    # Proxies públicos brasileiros - você pode adicionar mais
    "200.137.134.131:3128",
    "191.252.194.99:8080", 
    "179.184.224.91:3128",
    "177.38.76.153:8080"
]

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
        'service': 'testefinal-cloud-api-proxy',
        'version': '3.0.0',
        'proxy_enabled': True
    })

@app.route('/test', methods=['GET'])
def test_endpoint():
    """Endpoint de teste público"""
    return jsonify({
        'status': 'ok',
        'message': 'API com PROXY brasileiro funcionando!',
        'timestamp': datetime.now().isoformat(),
        'environment': 'cloud-proxy',
        'contahub_email': CONTAHUB_EMAIL,
        'sheet_name': SHEET_NAME,
        'proxies_available': len(BRAZILIAN_PROXIES)
    })

def get_session_with_proxy():
    """Cria sessão com proxy brasileiro"""
    session = requests.Session()
    
    # Tentar proxies brasileiros
    for proxy in BRAZILIAN_PROXIES:
        try:
            proxies = {
                'http': f'http://{proxy}',
                'https': f'http://{proxy}'
            }
            
            # Testar proxy com timeout curto
            test_response = requests.get('http://httpbin.org/ip', 
                                       proxies=proxies, 
                                       timeout=5)
            
            if test_response.status_code == 200:
                logger.info(f"Proxy brasileiro funcionando: {proxy}")
                session.proxies.update(proxies)
                return session, proxy
                
        except Exception as e:
            logger.warning(f"Proxy {proxy} falhou: {str(e)}")
            continue
    
    # Se nenhum proxy funcionar, usar conexão direta
    logger.warning("Nenhum proxy brasileiro funcionou, usando conexão direta")
    return session, None

def login_contahub_with_proxy():
    """Login no ContaHub usando proxy brasileiro"""
    try:
        logger.info(f"Tentando login no ContaHub com proxy brasileiro...")
        
        session, proxy_used = get_session_with_proxy()
        
        # Headers de browser brasileiro
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })
        
        payload = {
            "usr_email": CONTAHUB_EMAIL,
            "usr_senha": CONTAHUB_SENHA,
            "emp": 0
        }
        
        logger.info(f"Login com proxy: {proxy_used}")
        
        # Tentar login
        headers_login = {
            'Content-Type': 'application/json',
            'Origin': 'https://sp.contahub.com',
            'Referer': 'https://sp.contahub.com/'
        }
        
        response = session.post(LOGIN_URL, json=payload, headers=headers_login, timeout=30)
        
        logger.info(f"Status code: {response.status_code}")
        logger.info(f"Response: {response.text[:500]}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                
                if data.get('success'):
                    logger.info(f"✅ Login com proxy FUNCIONOU! Proxy: {proxy_used}")
                    return session, proxy_used
                else:
                    logger.error(f"❌ Login com proxy falhou: {data.get('message')}")
                    return None, proxy_used
                    
            except json.JSONDecodeError as e:
                logger.error(f"Erro JSON: {str(e)}")
                return None, proxy_used
        else:
            logger.error(f"Erro HTTP: {response.status_code}")
            return None, proxy_used
            
    except Exception as e:
        logger.error(f"Erro no login com proxy: {str(e)}")
        return None, None

def login_contahub_direct():
    """Login direto (sem proxy) como fallback"""
    try:
        logger.info("Tentando login direto (sem proxy)...")
        
        session = requests.Session()
        session.headers.update({
            'Content-Type': 'application/json'
        })
        
        payload = {
            "usr_email": CONTAHUB_EMAIL,
            "usr_senha": CONTAHUB_SENHA,
            "emp": 0
        }
        
        response = session.post(LOGIN_URL, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                logger.info("✅ Login direto funcionou!")
                return session
                
        logger.error("❌ Login direto falhou")
        return None
        
    except Exception as e:
        logger.error(f"Erro no login direto: {str(e)}")
        return None

@app.route('/debug-login-proxy', methods=['GET'])
def debug_login_proxy():
    """Debug do login com proxy"""
    try:
        debug_info = {
            'timestamp': datetime.now().isoformat(),
            'environment': 'cloud-proxy',
            'contahub_email': CONTAHUB_EMAIL,
            'proxies_tested': len(BRAZILIAN_PROXIES),
            'tests': {}
        }
        
        # Teste 1: Login com proxy
        logger.info("🔍 Teste 1: Login com proxy brasileiro")
        session_proxy, proxy_used = login_contahub_with_proxy()
        debug_info['tests']['proxy_login'] = {
            'success': session_proxy is not None,
            'proxy_used': proxy_used,
            'message': f'Login com proxy {"funcionou" if session_proxy else "falhou"} (proxy: {proxy_used})'
        }
        
        # Teste 2: Login direto
        logger.info("🔍 Teste 2: Login direto")
        session_direct = login_contahub_direct()
        debug_info['tests']['direct_login'] = {
            'success': session_direct is not None,
            'message': f'Login direto {"funcionou" if session_direct else "falhou"}'
        }
        
        # Resultado final
        any_success = session_proxy is not None or session_direct is not None
        
        if any_success:
            debug_info['overall_status'] = 'success'
            debug_info['message'] = 'Pelo menos um método funcionou!'
            debug_info['recommended_method'] = 'proxy' if session_proxy else 'direct'
            return jsonify(debug_info)
        else:
            debug_info['overall_status'] = 'error'
            debug_info['message'] = 'Todos os métodos falharam - possível bloqueio total'
            return jsonify(debug_info), 500
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/execute-testefinal-proxy', methods=['POST'])
@require_api_key
def execute_testefinal_proxy():
    """Executa testefinal usando proxy"""
    try:
        logger.info("🚀 Executando TesteFinal com PROXY brasileiro")
        
        # Tentar login com proxy primeiro
        session, proxy_used = login_contahub_with_proxy()
        
        if not session:
            # Fallback para login direto
            session = login_contahub_direct()
            proxy_used = "direct"
        
        if not session:
            return jsonify({
                'status': 'error',
                'error': 'Falha em todos os métodos de login',
                'timestamp': datetime.now().isoformat()
            }), 500
        
        logger.info(f"✅ Login realizado com sucesso usando: {proxy_used}")
        
        return jsonify({
            'status': 'success',
            'message': f'Login realizado com sucesso usando {proxy_used}',
            'proxy_used': proxy_used,
            'timestamp': datetime.now().isoformat(),
            'note': 'Implementação completa em desenvolvimento'
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
    
    print(f"🚀 API Cloud com PROXY brasileiro iniciando na porta {port}")
    print(f"📋 Endpoints disponíveis:")
    print(f"   GET  /health")
    print(f"   GET  /test")
    print(f"   GET  /debug-login-proxy")
    print(f"   POST /execute-testefinal-proxy")
    print(f"🔐 API Key: {API_KEY}")
    print(f"📧 ContaHub Email: {CONTAHUB_EMAIL}")
    print(f"🌐 Proxies disponíveis: {len(BRAZILIAN_PROXIES)}")
    
    app.run(host='0.0.0.0', port=port, debug=False) 