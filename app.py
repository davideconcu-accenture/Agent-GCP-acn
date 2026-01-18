"""
ETL Agent API - Semplice per Cloud Run
"""
from flask import Flask, request, jsonify
import os
from etl_agent.agent.safe_agent_orchestrator import SafeAgentOrchestrator, AgentLimits

app = Flask(__name__)

# Configurazione
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')

@app.route('/')
def home():
    """Health check"""
    return jsonify({
        "status": "ok",
        "service": "ETL Agent API",
        "version": "1.0.0"
    })

@app.route('/health')
def health():
    """Health check per Cloud Run"""
    return jsonify({"status": "healthy"}), 200

@app.route('/analyze', methods=['POST'])
def analyze():
    """
    Endpoint principale per analizzare segnalazioni
    
    Body JSON:
    {
        "task": "Descrizione del problema...",
        "max_cost": 0.5,
        "max_time": 90
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'task' not in data:
            return jsonify({
                "error": "Campo 'task' obbligatorio"
            }), 400
        
        task = data['task']
        max_cost = data.get('max_cost', 0.5)
        max_time = data.get('max_time', 90)
        
        # Crea agent con limiti
        limits = AgentLimits(
            max_iterations=10,
            max_total_cost=max_cost,
            max_time_seconds=max_time
        )
        
        agent = SafeAgentOrchestrator(
            api_key=ANTHROPIC_API_KEY,
            limits=limits,
            verbose=False  # Disabilita output console
        )
        
        # Esegui analisi
        result = agent.run(task)
        
        return jsonify({
            "success": result['success'],
            "solution": result['response'],
            "stats": {
                "cost": result['stats']['total_cost'],
                "time": result['stats']['elapsed_time'],
                "iterations": result['stats']['iterations'],
                "tools_used": result['stats']['tool_calls']
            }
        })
        
    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
