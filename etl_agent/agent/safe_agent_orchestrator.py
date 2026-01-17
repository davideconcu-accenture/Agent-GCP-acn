"""
Safe Agent Orchestrator - Agent con limiti di sicurezza
Previene loop infiniti, costi eccessivi e query pericolose
"""

import anthropic
from typing import Dict, List, Any, Optional
import time
import json
from collections import defaultdict
from dataclasses import dataclass, field
import sys

# Import tools
from etl_agent.tools.agent_tools import AgentTools


@dataclass
class AgentLimits:
    """Configurazione limiti di sicurezza"""
    max_iterations: int = 15
    max_claude_calls: int = 20
    max_total_cost: float = 2.0
    max_time_seconds: int = 120
    tool_limits: Dict[str, int] = field(default_factory=lambda: {
        "execute_sql_query": 5,
        "execute_python_code": 3,
        "read_sql_code": 2,
        "read_brb_requirements": 2,
        "read_quadratura_results": 2
    })
    sql_max_rows: int = 10000
    sql_timeout_seconds: int = 30
    sql_require_limit: bool = True
    sql_block_keywords: List[str] = field(default_factory=lambda: 
        ["DELETE", "DROP", "TRUNCATE", "UPDATE", "INSERT", "ALTER"]
    )


@dataclass
class AgentStats:
    """Statistiche esecuzione agent"""
    iterations: int = 0
    claude_calls: int = 0
    total_cost: float = 0.0
    tool_calls: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    start_time: float = field(default_factory=time.time)
    
    def elapsed_time(self) -> float:
        return time.time() - self.start_time


class LimitExceededError(Exception):
    """Eccezione quando un limite viene superato"""
    pass


class SafeAgentOrchestrator:
    """
    Orchestrator agent con limiti di sicurezza
    Gestisce loop agentic con Claude + tool use
    """
    
    # System prompt per l'agent
    SYSTEM_PROMPT = """Sei un esperto analista di codice SQL specializzato in ETL (Extract, Transform, Load).

Ricevi segnalazioni di problemi su pipeline ETL e devi investigare e risolvere.

APPROCCIO INVESTIGATIVO:
- Usa SOLO i tool necessari per risolvere il problema specifico
- Non fare analisi complete se non richieste
- Inizia dall'informazione più rilevante al problema
- Se la soluzione è chiara dopo pochi step, proponi subito il fix
- Sii efficiente: più veloce e economico possibile

STRATEGIA:
1. Analizza la segnalazione per capire il tipo di problema
2. Identifica quale informazione ti serve VERAMENTE
3. Usa i tool minimi necessari
4. Proponi fix concreto

ESEMPI:
- Problema "differenze importi" → Leggi quadratura → Identifica pattern → Proponi fix (NO: leggere tutto il codice se non serve)
- Problema "record mancanti" → Leggi quadratura per capire quanti → Se chiaro, proponi fix (NO: analizzare tutto il BRB)
- Problema "performance lenta" → Potrebbe servire codice, ma NON requisiti

PRIORITÀ:
- CRITICAL: perdita dati, calcoli errati, blocco pipeline
- HIGH: violazioni requisiti, performance degradate
- MEDIUM: code quality, standardizzazione
- LOW: ottimizzazioni minori

IMPORTANTE: Non sprecare token e tempo. Vai dritto al problema."""
    
    # Costi approssimativi (per stima)
    COST_PER_1K_TOKENS = {
        "input": 0.003,   # Claude Sonnet 4
        "output": 0.015
    }
    
    def __init__(
        self, 
        api_key: str,
        limits: Optional[AgentLimits] = None,
        verbose: bool = True
    ):
        """
        Inizializza Safe Agent
        
        Args:
            api_key: Anthropic API key
            limits: Limiti di sicurezza (usa default se None)
            verbose: Stampa progress durante esecuzione
        """
        self.api_key = api_key
        self.limits = limits or AgentLimits()
        self.verbose = verbose
        
        # Inizializza Claude client
        self.client = anthropic.Anthropic(api_key=api_key)
        
        # Inizializza tools
        self.tools = AgentTools(mode="local")
        
        # Stats
        self.stats = AgentStats()
        
        # Conversation history per multi-turn
        self.conversation_history = []
    
    def _print(self, message: str):
        """Print se verbose è attivo"""
        if self.verbose:
            print(message)
    
    def _check_limits(self):
        """Verifica che nessun limite sia stato superato"""
        
        # Iteration limit
        if self.stats.iterations >= self.limits.max_iterations:
            raise LimitExceededError(
                f"⛔ Max iterations raggiunto ({self.limits.max_iterations})"
            )
        
        # Claude calls limit
        if self.stats.claude_calls >= self.limits.max_claude_calls:
            raise LimitExceededError(
                f"⛔ Max chiamate Claude raggiunto ({self.limits.max_claude_calls})"
            )
        
        # Cost limit
        if self.stats.total_cost >= self.limits.max_total_cost:
            raise LimitExceededError(
                f"⛔ Budget superato (${self.stats.total_cost:.2f}/${self.limits.max_total_cost})"
            )
        
        # Time limit
        if self.stats.elapsed_time() >= self.limits.max_time_seconds:
            raise LimitExceededError(
                f"⛔ Timeout ({self.stats.elapsed_time():.0f}s/{self.limits.max_time_seconds}s)"
            )
    
    def _check_tool_limit(self, tool_name: str):
        """Verifica limite per tool specifico"""
        limit = self.limits.tool_limits.get(tool_name, 999)
        current = self.stats.tool_calls[tool_name]
        
        if current >= limit:
            raise LimitExceededError(
                f"⛔ Tool '{tool_name}' limit raggiunto ({current}/{limit})"
            )
    
    def _estimate_cost(self, response) -> float:
        """Stima costo chiamata Claude"""
        usage = response.usage
        
        input_cost = (usage.input_tokens / 1000) * self.COST_PER_1K_TOKENS["input"]
        output_cost = (usage.output_tokens / 1000) * self.COST_PER_1K_TOKENS["output"]
        
        return input_cost + output_cost
    
    def _validate_sql(self, sql_query: str):
        """Valida query SQL prima dell'esecuzione"""
        sql_upper = sql_query.upper()
        
        # Blocca keywords pericolose
        for keyword in self.limits.sql_block_keywords:
            if keyword in sql_upper:
                raise LimitExceededError(
                    f"⛔ Query SQL contiene keyword bloccata: {keyword}"
                )
        
        # Richiedi LIMIT se configurato
        if self.limits.sql_require_limit and "LIMIT" not in sql_upper:
            raise LimitExceededError(
                f"⛔ Query SQL deve includere LIMIT (max {self.limits.sql_max_rows} righe)"
            )
    
    def _display_progress(self):
        """Mostra progress bar con stats"""
        if not self.verbose:
            return
        
        elapsed = self.stats.elapsed_time()
        
        print(f"\n{'='*60}")
        print(f"🤖 Agent Progress")
        print(f"{'='*60}")
        print(f"⏱️  Tempo: {elapsed:.1f}s / {self.limits.max_time_seconds}s")
        print(f"🔄 Iterazioni: {self.stats.iterations} / {self.limits.max_iterations}")
        print(f"💬 Chiamate Claude: {self.stats.claude_calls} / {self.limits.max_claude_calls}")
        print(f"💰 Costo stimato: ${self.stats.total_cost:.4f} / ${self.limits.max_total_cost}")
        
        if self.stats.tool_calls:
            print(f"\n🔧 Tool chiamati:")
            for tool, count in self.stats.tool_calls.items():
                limit = self.limits.tool_limits.get(tool, "∞")
                print(f"   • {tool}: {count}/{limit}")
        
        print(f"{'='*60}\n")
    
    def run(self, task: str, model: str = "claude-sonnet-4-20250514") -> Dict[str, Any]:
        """
        Esegue agent loop per completare un task
        
        Args:
            task: Task da completare (es. "Analizza ETL vendite e proponi fix")
            model: Modello Claude da usare
            
        Returns:
            Dict con risultati e statistiche
        """
        
        self._print(f"\n{'='*60}")
        self._print(f"🚀 Avvio Agent")
        self._print(f"{'='*60}")
        self._print(f"Task: {task}")
        self._print(f"Limiti: {self.limits.max_iterations} iter, ${self.limits.max_total_cost} budget")
        self._print(f"{'='*60}\n")
        
        # Messaggio iniziale
        self.conversation_history = [
            {
                "role": "user",
                "content": task
            }
        ]
        
        # Agent loop
        final_response = None
        
        try:
            while True:
                self.stats.iterations += 1
                self._check_limits()
                
                self._print(f"\n🔄 Iterazione {self.stats.iterations}")
                
                # Chiama Claude con tool use
                response = self.client.messages.create(
                    model=model,
                    max_tokens=4096,
                    system=self.SYSTEM_PROMPT,  # ← System prompt!
                    tools=self.tools.get_tool_definitions(),
                    messages=self.conversation_history
                )
                
                # Update stats
                self.stats.claude_calls += 1
                self.stats.total_cost += self._estimate_cost(response)
                
                # Gestisci risposta
                stop_reason = response.stop_reason
                
                if stop_reason == "end_turn":
                    # Agent ha finito
                    final_response = response.content[0].text
                    self._print("\n✅ Agent ha completato il task!")
                    break
                
                elif stop_reason == "tool_use":
                    # Agent vuole usare tool
                    tool_uses = [block for block in response.content if block.type == "tool_use"]
                    
                    self._print(f"🔧 Agent richiede {len(tool_uses)} tool:")
                    
                    # Esegui tutti i tool richiesti
                    tool_results = []
                    
                    for tool_use in tool_uses:
                        tool_name = tool_use.name
                        tool_input = tool_use.input
                        
                        self._print(f"   • {tool_name}({json.dumps(tool_input, indent=2)[:100]}...)")
                        
                        # Check limite tool
                        self._check_tool_limit(tool_name)
                        
                        # Validazione speciale per SQL
                        if tool_name == "execute_sql_query" and "query" in tool_input:
                            self._validate_sql(tool_input["query"])
                        
                        # Esegui tool
                        result = self.tools.execute_tool(tool_name, tool_input)
                        
                        # Update stats
                        self.stats.tool_calls[tool_name] += 1
                        
                        # Aggiungi risultato
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": tool_use.id,
                            "content": json.dumps(result) if isinstance(result, dict) else str(result)
                        })
                    
                    # Aggiungi alla conversazione
                    self.conversation_history.append({
                        "role": "assistant",
                        "content": response.content
                    })
                    
                    self.conversation_history.append({
                        "role": "user",
                        "content": tool_results
                    })
                    
                    # Mostra progress
                    self._display_progress()
                
                else:
                    # Stop reason inaspettato
                    self._print(f"⚠️ Stop reason inaspettato: {stop_reason}")
                    break
        
        except LimitExceededError as e:
            self._print(f"\n{str(e)}")
            final_response = f"Agent fermato: {str(e)}"
        
        except KeyboardInterrupt:
            self._print("\n\n⚠️ Esecuzione interrotta dall'utente (Ctrl+C)")
            final_response = "Esecuzione interrotta dall'utente"
        
        except Exception as e:
            self._print(f"\n❌ Errore durante esecuzione: {e}")
            import traceback
            traceback.print_exc()
            final_response = f"Errore: {str(e)}"
        
        # Report finale
        self._print(f"\n{'='*60}")
        self._print(f"📊 Report Finale")
        self._print(f"{'='*60}")
        self._print(f"⏱️  Tempo totale: {self.stats.elapsed_time():.1f}s")
        self._print(f"🔄 Iterazioni: {self.stats.iterations}")
        self._print(f"💬 Chiamate Claude: {self.stats.claude_calls}")
        self._print(f"💰 Costo totale: ${self.stats.total_cost:.4f}")
        self._print(f"🔧 Tool chiamati: {sum(self.stats.tool_calls.values())}")
        self._print(f"{'='*60}\n")
        
        return {
            "success": final_response is not None,
            "response": final_response,
            "stats": {
                "iterations": self.stats.iterations,
                "claude_calls": self.stats.claude_calls,
                "total_cost": self.stats.total_cost,
                "elapsed_time": self.stats.elapsed_time(),
                "tool_calls": dict(self.stats.tool_calls)
            },
            "conversation_history": self.conversation_history
        }


# Test del modulo
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    print("\n🧪 Test SafeAgentOrchestrator\n")
    
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        print("❌ ANTHROPIC_API_KEY non trovato")
        exit(1)
    
    # Crea agent con limiti conservativi per test
    test_limits = AgentLimits(
        max_iterations=5,
        max_total_cost=0.50,  # Max $0.50 per test
        max_time_seconds=60
    )
    
    agent = SafeAgentOrchestrator(
        api_key=api_key,
        limits=test_limits,
        verbose=True
    )
    
    # Test task semplice
    task = """
    Analizza l'ETL vendite:
    1. Leggi il codice SQL
    2. Leggi i requisiti BRB
    3. Identifica i 3 problemi principali
    4. Proponi fix per il primo problema
    """
    
    result = agent.run(task)
    
    print("\n" + "="*60)
    print("RISULTATO FINALE:")
    print("="*60)
    print(result["response"][:500] + "...")
    print("\n✅ Test completato!")
