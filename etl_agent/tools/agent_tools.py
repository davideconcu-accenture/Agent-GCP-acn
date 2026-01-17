"""
Agent Tools - Tool disponibili per l'agent
Ogni tool può essere chiamato autonomamente da Claude
"""

from typing import Dict, Any, List
from etl_agent.tools.storage_reader import StorageReader
from etl_agent.tools.excel_analyzer import ExcelAnalyzer
import json

class AgentTools:
    """Classe che definisce tutti i tool disponibili per l'agent"""
    
    def __init__(self, mode: str = "local", project_id: str = None):
        """
        Inizializza i tool
        
        Args:
            mode: "local" o "cloud"
            project_id: ID progetto GCP (per mode="cloud")
        """
        self.storage_reader = StorageReader(mode=mode, project_id=project_id)
        
        # Cache per evitare letture ripetute
        self._cache = {}
    
    def get_tool_definitions(self) -> List[Dict]:
        """
        Restituisce le definizioni dei tool in formato Claude API
        
        Returns:
            Lista di definizioni tool per Claude
        """
        return [
            {
                "name": "read_sql_code",
                "description": "Legge il codice SQL di un ETL specifico. Usa questo tool quando hai bisogno di vedere il codice da analizzare.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "etl_name": {
                            "type": "string",
                            "description": "Nome dell'ETL (es. 'etl_vendite', 'etl_ordini')"
                        }
                    },
                    "required": ["etl_name"]
                }
            },
            {
                "name": "read_brb_requirements",
                "description": "Legge i Business Requirements Baseline (BRB) di un ETL. Contiene regole di business, KPI, sorgenti e target. Usa questo quando devi capire i requisiti.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "etl_name": {
                            "type": "string",
                            "description": "Nome dell'ETL"
                        }
                    },
                    "required": ["etl_name"]
                }
            },
            {
                "name": "read_quadratura_results",
                "description": "Legge i risultati della quadratura (confronto vecchio vs nuovo workflow). Mostra match %, squadrature e campi differenti. Usa questo per capire quali problemi ci sono.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "etl_name": {
                            "type": "string",
                            "description": "Nome dell'ETL"
                        }
                    },
                    "required": ["etl_name"]
                }
            },
            {
                "name": "list_available_etls",
                "description": "Lista tutti gli ETL disponibili per l'analisi. Usa questo se non sai quale ETL analizzare o vuoi vedere cosa è disponibile.",
                "input_schema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "analyze_code_section",
                "description": "Analizza una sezione specifica del codice SQL in dettaglio. Usa questo quando hai identificato un'area problematica e vuoi analizzarla meglio.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "code_section": {
                            "type": "string",
                            "description": "Sezione di codice da analizzare"
                        },
                        "context": {
                            "type": "string",
                            "description": "Contesto o cosa cercare (es. 'validazione quantità', 'calcolo importi')"
                        }
                    },
                    "required": ["code_section", "context"]
                }
            },
            {
                "name": "validate_sql_syntax",
                "description": "Valida la sintassi SQL di un codice proposto. Usa questo per verificare che il codice corretto sia sintatticamente valido.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "sql_code": {
                            "type": "string",
                            "description": "Codice SQL da validare"
                        }
                    },
                    "required": ["sql_code"]
                }
            },
            {
                "name": "execute_python_code",
                "description": "Esegue codice Python personalizzato quando hai bisogno di funzionalità non coperte dai tool esistenti. IMPORTANTE: Usa questo solo quando i tool predefiniti non sono sufficienti. Il codice viene eseguito in un ambiente sicuro con accesso a pandas, json, re, math, datetime. Restituisce stdout e risultato finale.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "code": {
                            "type": "string",
                            "description": "Codice Python da eseguire. Deve includere un 'result' finale da restituire."
                        },
                        "purpose": {
                            "type": "string",
                            "description": "Breve descrizione di cosa fa il codice (per logging)"
                        }
                    },
                    "required": ["code", "purpose"]
                }
            },
            {
                "name": "execute_sql_query",
                "description": "Esegue query SQL su database di test in-memory (SQLite). Usa questo per testare query, validare ipotesi sui dati, o verificare che le modifiche proposte funzionino. Il database contiene dati di esempio simulati. IMPORTANTE: Le query devono essere READ-ONLY (solo SELECT) e DEVONO includere LIMIT (es. LIMIT 100) per motivi di sicurezza - query senza LIMIT verranno rifiutate.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Query SQL da eseguire (solo SELECT, DEVE includere LIMIT)"
                        },
                        "purpose": {
                            "type": "string",
                            "description": "Cosa vuoi testare/verificare con questa query"
                        },
                        "create_sample_data": {
                            "type": "boolean",
                            "description": "Se true, crea dati di esempio prima di eseguire la query"
                        }
                    },
                    "required": ["query", "purpose"]
                }
            }
        ]
    
    def execute_tool(self, tool_name: str, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Esegue un tool specifico
        
        Args:
            tool_name: Nome del tool da eseguire
            tool_input: Input per il tool
            
        Returns:
            Risultato dell'esecuzione del tool
        """
        
        # Map tool name to method
        tool_methods = {
            "read_sql_code": self._read_sql_code,
            "read_brb_requirements": self._read_brb_requirements,
            "read_quadratura_results": self._read_quadratura_results,
            "list_available_etls": self._list_available_etls,
            "analyze_code_section": self._analyze_code_section,
            "validate_sql_syntax": self._validate_sql_syntax,
            "execute_python_code": self._execute_python_code,
            "execute_sql_query": self._execute_sql_query
        }
        
        if tool_name not in tool_methods:
            return {
                "success": False,
                "error": f"Tool '{tool_name}' non trovato"
            }
        
        try:
            result = tool_methods[tool_name](tool_input)
            return {
                "success": True,
                "result": result
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    # ===== Implementazione dei singoli tool =====
    
    def _read_sql_code(self, input_data: Dict) -> str:
        """Legge il codice SQL"""
        etl_name = input_data["etl_name"]
        
        cache_key = f"sql_{etl_name}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        sql_code, path = self.storage_reader.read_sql_file(etl_name)
        
        result = f"""Codice SQL per {etl_name}:
Path: {path}
Lunghezza: {len(sql_code)} caratteri

--- CODICE SQL ---
{sql_code}
--- FINE CODICE ---
"""
        
        self._cache[cache_key] = result
        return result
    
    def _read_brb_requirements(self, input_data: Dict) -> str:
        """Legge i requisiti BRB"""
        etl_name = input_data["etl_name"]
        
        cache_key = f"brb_{etl_name}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        brb_data, path = self.storage_reader.read_brb_excel(etl_name)
        brb_analysis = ExcelAnalyzer.analyze_brb(brb_data)
        
        result = f"""Business Requirements Baseline per {etl_name}:
Path: {path}

ETL: {brb_analysis.etl_name}
Versione: {brb_analysis.version}
Owner: {brb_analysis.owner}
Frequenza: {brb_analysis.frequency}
Obiettivo: {brb_analysis.objective}

REGOLE DI BUSINESS ({len(brb_analysis.business_rules)} regole):
"""
        for rule in brb_analysis.business_rules:
            result += f"\n{rule['id']} [{rule['criticita']}]: {rule['descrizione']}"
            if rule['formula']:
                result += f"\n  Formula: {rule['formula']}"
        
        result += f"\n\nKPI E SOGLIE ({len(brb_analysis.kpis)} KPI):\n"
        for kpi in brb_analysis.kpis:
            result += f"\n{kpi['id']}: {kpi['descrizione']}"
            result += f"\n  Metrica: {kpi['metrica']}"
            result += f"\n  Soglia: {kpi['soglia']}"
        
        self._cache[cache_key] = result
        return result
    
    def _read_quadratura_results(self, input_data: Dict) -> str:
        """Legge i risultati della quadratura"""
        etl_name = input_data["etl_name"]
        
        cache_key = f"quad_{etl_name}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        quad_data, path = self.storage_reader.read_quadratura_excel(etl_name)
        quad_analysis = ExcelAnalyzer.analyze_quadratura(quad_data)
        
        result = f"""Risultati Quadratura per {etl_name}:
Path: {path}

SUMMARY:
- Match: {quad_analysis.match_percentage}%
- Record totali vecchio: {quad_analysis.total_old}
- Record totali nuovo: {quad_analysis.total_new}
- Record in match: {quad_analysis.records_match}
- Record solo in vecchio: {quad_analysis.records_only_old}
- Record solo in nuovo: {quad_analysis.records_only_new}

PROBLEMI IDENTIFICATI ({len(quad_analysis.different_fields)} campi con differenze):
"""
        
        # Raggruppa per tipo di differenza
        problems_by_type = {}
        for diff in quad_analysis.different_fields:
            tipo = diff['tipo_differenza']
            if tipo not in problems_by_type:
                problems_by_type[tipo] = []
            problems_by_type[tipo].append(diff)
        
        for tipo, diffs in problems_by_type.items():
            result += f"\n• {tipo} ({len(diffs)} casi):"
            for diff in diffs[:5]:
                result += f"\n  - ID: {diff['id']}, Campo: {diff['campo']}"
                result += f"\n    Vecchio: '{diff['valore_vecchio']}' → Nuovo: '{diff['valore_nuovo']}'"
        
        if quad_analysis.squadrature:
            result += f"\n\nSQUADRATURE ({len(quad_analysis.squadrature)}):"
            for sq in quad_analysis.squadrature[:10]:
                result += f"\n• {sq['tipo']}: {sq['id']} - {sq['descrizione']}"
        
        self._cache[cache_key] = result
        return result
    
    def _list_available_etls(self, input_data: Dict) -> str:
        """Lista ETL disponibili"""
        etls = self.storage_reader.list_available_etls()
        
        result = f"ETL disponibili per l'analisi ({len(etls)}):\n"
        for etl in etls:
            result += f"- {etl}\n"
        
        return result
    
    def _analyze_code_section(self, input_data: Dict) -> str:
        """Analizza una sezione di codice in dettaglio"""
        code_section = input_data["code_section"]
        context = input_data["context"]
        
        lines = code_section.split('\n')
        
        result = f"""Analisi dettagliata della sezione di codice:
Contesto: {context}
Linee di codice: {len(lines)}

Osservazioni:
"""
        
        if "CONCAT" in code_section.upper():
            result += "- Trovata concatenazione di stringhe (CONCAT)\n"
        if "ROUND" not in code_section.upper() and any(op in code_section for op in ['*', '/']):
            result += "- Calcoli numerici senza arrotondamento esplicito\n"
        if "WHERE" in code_section.upper():
            result += "- Presente clausola WHERE (filtro)\n"
        if "JOIN" in code_section.upper():
            result += "- Presente JOIN\n"
        
        result += f"\n--- CODICE ANALIZZATO ---\n{code_section}\n--- FINE ---"
        
        return result
    
    def _validate_sql_syntax(self, input_data: Dict) -> str:
        """Valida sintassi SQL (validazione base)"""
        sql_code = input_data["sql_code"]
        
        issues = []
        
        if sql_code.count('(') != sql_code.count(')'):
            issues.append("Parentesi non bilanciate")
        
        sql_upper = sql_code.upper()
        if 'SELECT' not in sql_upper and 'CREATE' not in sql_upper and 'INSERT' not in sql_upper:
            issues.append("Nessuna keyword SQL principale trovata")
        
        if not sql_code.strip().endswith(';'):
            issues.append("Punto e virgola finale mancante (consigliato)")
        
        if issues:
            result = "⚠️ Problemi di sintassi rilevati:\n"
            for issue in issues:
                result += f"- {issue}\n"
        else:
            result = "✓ Sintassi SQL sembra valida (validazione base)"
        
        return result
    
    def _execute_python_code(self, input_data: Dict) -> str:
        """Esegue codice Python in ambiente controllato"""
        code = input_data["code"]
        purpose = input_data.get("purpose", "Esecuzione codice custom")
        
        print(f"🐍 Esecuzione codice Python: {purpose}")
        
        safe_globals = {
            "__builtins__": {
                "len": len, "str": str, "int": int, "float": float, "bool": bool,
                "list": list, "dict": dict, "set": set, "tuple": tuple,
                "range": range, "enumerate": enumerate, "zip": zip,
                "map": map, "filter": filter, "sorted": sorted,
                "sum": sum, "min": min, "max": max, "abs": abs,
                "round": round, "print": print,
            },
            "json": __import__("json"),
            "re": __import__("re"),
            "math": __import__("math"),
            "datetime": __import__("datetime"),
        }
        
        try:
            import pandas as pd
            safe_globals["pd"] = pd
            safe_globals["pandas"] = pd
        except ImportError:
            pass
        
        local_vars = {}
        
        from io import StringIO
        import sys
        
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            exec(code, safe_globals, local_vars)
            sys.stdout = old_stdout
            output = captured_output.getvalue()
            result = local_vars.get('result', None)
            
            response = f"""Esecuzione completata: {purpose}

Output:
{output if output else '(nessun output)'}

Risultato finale:
{result if result is not None else '(nessun result definito)'}
"""
            
            print(f"✓ Codice eseguito con successo")
            return response
            
        except Exception as e:
            sys.stdout = old_stdout
            error_msg = f"""Errore durante l'esecuzione: {purpose}

Errore: {type(e).__name__}: {str(e)}

Il codice che ha causato l'errore:
{code}
"""
            print(f"✗ Errore nell'esecuzione: {e}")
            return error_msg
    
    def _execute_sql_query(self, input_data: Dict) -> str:
        """Esegue query SQL su database SQLite in-memory"""
        query = input_data["query"]
        purpose = input_data.get("purpose", "Test query SQL")
        create_sample = input_data.get("create_sample_data", True)
        
        print(f"🗄️  Esecuzione query SQL: {purpose}")
        
        import sqlite3
        
        # Auto-fix: aggiungi LIMIT se manca e la query è SELECT
        if "LIMIT" not in query.upper() and "SELECT" in query.upper():
            query = query.rstrip(';').rstrip() + " LIMIT 1000"
            print(f"   ℹ️  LIMIT aggiunto automaticamente (sicurezza)")
        
        try:
            conn = sqlite3.connect(':memory:')
            cursor = conn.cursor()
            
            if create_sample:
                cursor.execute('''
                    CREATE TABLE vendite (
                        id_vendita INTEGER PRIMARY KEY,
                        data_vendita TEXT,
                        id_cliente INTEGER,
                        nome_cliente TEXT,
                        cognome_cliente TEXT,
                        regione TEXT,
                        id_prodotto INTEGER,
                        nome_prodotto TEXT,
                        categoria TEXT,
                        quantita INTEGER,
                        prezzo_unitario REAL,
                        sconto INTEGER
                    )
                ''')
                
                sample_data = [
                    (1, '2025-01-15', 101, 'Mario', 'Rossi', 'Lombardia', 201, 'Prodotto A', 'Cat1', 5, 100.00, 10),
                    (2, '2025-01-15', 102, 'Luigi', 'Verdi', 'Lazio', 202, 'Prodotto B', 'Cat2', 3, 150.50, 5),
                    (3, '2025-01-15', 103, 'Anna', 'Bianchi', 'Piemonte', 203, 'Prodotto C', 'Cat1', 10, 75.25, 15),
                    (4, '2025-01-15', 101, 'Mario', 'Rossi', 'Lombardia', 204, 'Prodotto D', 'Cat3', 2, 200.00, 0),
                ]
                
                cursor.executemany('INSERT INTO vendite VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', sample_data)
                conn.commit()
            
            cursor.execute(query)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            
            response = f"""Query eseguita: {purpose}

Query SQL:
{query}

Risultati ({len(results)} righe):
"""
            
            if column_names:
                response += "\n" + " | ".join(column_names) + "\n"
                response += "-" * (len(" | ".join(column_names))) + "\n"
                
                for row in results[:20]:
                    response += " | ".join(str(val) for val in row) + "\n"
                
                if len(results) > 20:
                    response += f"\n... e altre {len(results) - 20} righe"
            else:
                response += "(Nessun risultato)"
            
            conn.close()
            
            print(f"✓ Query eseguita: {len(results)} righe")
            return response
            
        except sqlite3.Error as e:
            error_msg = f"""Errore SQL: {purpose}

Query che ha causato l'errore:
{query}

Errore: {str(e)}
"""
            print(f"✗ Errore SQL: {e}")
            return error_msg
        
        finally:
            try:
                conn.close()
            except:
                pass