from etl_agent.agent.safe_agent_orchestrator import SafeAgentOrchestrator, AgentLimits
from dotenv import load_dotenv
import os

load_dotenv()

# Esempi di segnalazioni (diverse gravità)
SEGNALAZIONI = {
    "importi": """
Ci è arrivata questa segnalazione sull'ETL vendite:

"Buongiorno team,
nella quadratura di ieri abbiamo notato differenze sugli importi netti 
tra vecchio e nuovo workflow. Alcuni record hanno differenze di pochi 
centesimi (es: vecchio=150.00, nuovo=149.99).
Potete verificare cosa causa queste differenze?

Grazie,
Business Analyst"

Analizza e risolvi il problema.
""",
    
    "record_mancanti": """
Segnalazione URGENTE sull'ETL vendite:

"Attenzione! La quadratura di oggi mostra 60 record presenti solo 
nel vecchio workflow e assenti nel nuovo. Questo potrebbe indicare 
perdita di dati.

Verificare immediatamente la causa.

Team QA"

Investiga e proponi soluzione.
""",
    
    "formattazione": """
Segnalazione minore ETL vendite:

"Ciao,
ho notato che nel campo 'cliente_completo' alcuni record hanno 
spazi doppi tra nome e cognome (es: 'Mario  Rossi'). 
Non è bloccante ma sarebbe bene sistemare.

Grazie"

Se è veloce da fixare, proponi soluzione.
""",
    
    "performance": """
Alert performance ETL vendite:

"L'ETL vendite sta impiegando 15 minuti invece dei soliti 5.
Non ci sono errori ma è molto più lento del normale.

Può essere un problema di query non ottimizzata?

DevOps Team"

Analizza possibili cause e suggerimenti.
""",
    
    "generico": """
"Ciao, l'ETL vendite non funziona bene, riesci a dare un'occhiata?"
""",
}


if __name__ == "__main__":
    
    api_key = os.getenv('ANTHROPIC_API_KEY')
    
    if not api_key:
        print("❌ ANTHROPIC_API_KEY non trovato in .env")
        exit(1)
    
    # Configura limiti
    limits = AgentLimits(
        max_iterations=10,
        max_total_cost=0.50,
        max_time_seconds=90
    )
    
    # Crea agent
    agent = SafeAgentOrchestrator(
        api_key=api_key,
        limits=limits,
        verbose=True
    )
    
    # ============================================
    # 🎯 CAMBIA QUI per testare diverse segnalazioni
    # ============================================
    
    TIPO = "importi"  # ← MODIFICA QUESTA RIGA!
    # Opzioni: "importi", "record_mancanti", "formattazione", "performance", "generico"
    
    task = SEGNALAZIONI[TIPO]
    
    print(f"\n🎯 Testing segnalazione tipo: {TIPO.upper()}")
    print("="*60)
    
    # Esegui agent
    result = agent.run(task)
    
    # Report efficienza
    print("\n" + "="*60)
    print("📊 EFFICIENZA AGENT")
    print("="*60)
    print(f"⏱️  Tempo: {result['stats']['elapsed_time']:.1f}s")
    print(f"🔧 Tool chiamati: {sum(result['stats']['tool_calls'].values())}")
    print(f"💰 Costo: ${result['stats']['total_cost']:.4f}")
    print(f"🔄 Iterazioni: {result['stats']['iterations']}")
    
    # Tool details
    if result['stats']['tool_calls']:
        print(f"\n🔧 Dettaglio tool:")
        for tool, count in result['stats']['tool_calls'].items():
            print(f"   • {tool}: {count}x")
    
    print("\n" + "="*60)
    print("💡 SOLUZIONE PROPOSTA")
    print("="*60)
    if result["response"]:
        print(result["response"])
    
    print("\n✅ Completato!")
