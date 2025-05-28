#!/usr/bin/env python3
"""
API Cloud para executar testefinal.py via n8n Cloud
Versão otimizada para Railway/Render - 100% Cloud
"""
import os
import sys
import subprocess
import logging
import tempfile
import base64
from datetime import datetime
from flask import Flask, request, jsonify
from functools import wraps

# Configuração
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações de segurança
API_KEY = os.getenv('API_KEY', 'minha_chave_secreta_testefinal_2025')

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
        'service': 'testefinal-cloud-api',
        'version': '1.0.0'
    })

@app.route('/execute-testefinal', methods=['POST'])
@require_api_key
def execute_testefinal():
    """Endpoint para executar testefinal.py"""
    try:
        # Log da requisição
        logger.info(f"Executando testefinal.py às {datetime.now()}")
        
        # Simular execução do testefinal.py (substitua pela lógica real)
        # Por enquanto, vamos simular uma execução bem-sucedida
        
        # TODO: Aqui você colaria o código principal do seu testefinal.py
        # Por exemplo:
        result = simulate_testefinal_execution()
        
        if result['success']:
            logger.info("TesteFinal executado com sucesso")
            return jsonify({
                'status': 'success',
                'message': 'TesteFinal executado com sucesso',
                'timestamp': datetime.now().isoformat(),
                'data': result.get('data', {}),
                'execution_time': datetime.now().isoformat()
            })
        else:
            error_msg = result.get('error', 'Erro desconhecido na execução')
            logger.error(f"Erro na execução: {error_msg}")
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

def simulate_testefinal_execution():
    """
    Simula a execução do testefinal.py
    TODO: Substitua esta função pela lógica real do seu script
    """
    try:
        # Aqui você colaria as funções principais do testefinal.py
        # Por exemplo:
        
        # 1. Conectar com Google Sheets
        # 2. Processar dados
        # 3. Fazer requisições necessárias
        # 4. Retornar resultado
        
        # Por enquanto, simulando sucesso
        import time
        time.sleep(2)  # Simular processamento
        
        return {
            'success': True,
            'data': {
                'processed_items': 10,
                'sheets_updated': True,
                'execution_date': datetime.now().isoformat()
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

@app.route('/logs', methods=['GET'])
@require_api_key
def get_logs():
    """Endpoint para ver logs recentes"""
    try:
        # Em ambiente cloud, os logs vão para stdout/stderr
        # que são capturados pelo sistema de logging da plataforma
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

@app.route('/test', methods=['GET'])
def test_endpoint():
    """Endpoint de teste público"""
    return jsonify({
        'status': 'ok',
        'message': 'API funcionando perfeitamente!',
        'timestamp': datetime.now().isoformat(),
        'environment': 'cloud'
    })

if __name__ == '__main__':
    # Configurações para cloud
    port = int(os.getenv('PORT', 5000))
    
    print(f"🚀 API Cloud iniciando na porta {port}")
    print(f"📋 Endpoints disponíveis:")
    print(f"   GET  /health")
    print(f"   GET  /test")
    print(f"   POST /execute-testefinal")
    print(f"   GET  /logs")
    print(f"🔐 API Key: {API_KEY}")
    print(f"🌐 Ambiente: Cloud")
    
    app.run(host='0.0.0.0', port=port, debug=False) 