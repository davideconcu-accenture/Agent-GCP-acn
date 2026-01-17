from etl_agent.agent.safe_agent_orchestrator import SafeAgentOrchestrator, AgentLimits
from dotenv import load_dotenv
import os

load_dotenv()

if __name__ == "__main__":
    api_key = os.getenv('ANTHROPIC_API_KEY')
    
    if not api_key:
        print("❌ ANTHROPIC_API_KEY non trovato in .env")
        exit(1)
    
    test_limits = AgentLimits(
        max_iterations=8,
        max_total_cost=0.5,
        max_time_seconds=120
    )
    
    agent = SafeAgentOrchestrator(
        api_key=api_key,
        limits=test_limits,
        verbose=True
    )
    
    task = """
    Analizza l'ETL vendite:
    1. Leggi il codice SQL
    2. Leggi i requisiti BRB
    3. Leggi la quadratura
    4. Identifica i 3 problemi più critici
    5. Proponi fix per ognuno
    """
    
    result = agent.run(task)
    
    print("\n" + "="*60)
    print("RISULTATO FINALE:")
    print("="*60)
    if result["response"]:
        print(result["response"][:1000])
    print("\n✅ Agent completato!")
