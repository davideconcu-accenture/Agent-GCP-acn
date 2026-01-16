"""
Code Analyzer - Usa Anthropic Claude API diretta per analizzare SQL
"""

import anthropic
from typing import Dict, List
import json
from dataclasses import dataclass
import os

@dataclass
class CodeIssue:
    """Singolo problema trovato nel codice"""
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW
    title: str
    description: str
    file: str
    line_number: int
    rule_violated: str
    current_code: str
    proposed_fix: str
    explanation: str

@dataclass
class CodeAnalysisResult:
    """Risultato completo dell'analisi"""
    etl_name: str
    issues: List[CodeIssue]
    compliance_summary: Dict
    recommendations: List[str]
    fixed_sql: str
    analysis_time: float


class CodeAnalyzer:
    """Analizza codice SQL usando Anthropic Claude API"""
    
    def __init__(self, api_key: str, model_name: str = "claude-sonnet-4-20250514"):
        """
        Inizializza l'analyzer con Claude API
        
        Args:
            api_key: Anthropic API key
            model_name: Nome del modello Claude
        """
        self.api_key = api_key
        self.model_name = model_name
        self.client = anthropic.Anthropic(api_key=api_key)
    
    def analyze_sql(
        self,
        sql_code: str,
        brb_context: str,
        quadratura_context: str,
        etl_name: str
    ) -> CodeAnalysisResult:
        """
        Analizza il codice SQL e propone modifiche
        
        Args:
            sql_code: Codice SQL da analizzare
            brb_context: Contesto BRB formattato (da excel_analyzer)
            quadratura_context: Contesto quadratura formattato
            etl_name: Nome dell'ETL
            
        Returns:
            CodeAnalysisResult con problemi e proposte
        """
        
        print(f"🤖 Analisi SQL con Claude API...")
        
        # Costruisci il prompt per Claude
        prompt = self._build_analysis_prompt(
            sql_code=sql_code,
            brb_context=brb_context,
            quadratura_context=quadratura_context,
            etl_name=etl_name
        )
        
        # Chiama Claude API
        import time
        start_time = time.time()
        
        response = self.client.messages.create(
            model=self.model_name,
            max_tokens=8000,
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )
        
        analysis_time = time.time() - start_time
        
        # Estrai il contenuto della risposta
        response_text = response.content[0].text
        
        # Parse della risposta JSON
        analysis_data = self._parse_response(response_text)
        
        # Costruisci risultato
        issues = []
        for issue_data in analysis_data.get('issues', []):
            issues.append(CodeIssue(
                severity=issue_data.get('severity', 'MEDIUM'),
                title=issue_data.get('title', ''),
                description=issue_data.get('description', ''),
                file=f"{etl_name}.sql",
                line_number=issue_data.get('line_number', 0),
                rule_violated=issue_data.get('rule_violated', ''),
                current_code=issue_data.get('current_code', ''),
                proposed_fix=issue_data.get('proposed_fix', ''),
                explanation=issue_data.get('explanation', '')
            ))
        
        result = CodeAnalysisResult(
            etl_name=etl_name,
            issues=issues,
            compliance_summary=analysis_data.get('compliance_summary', {}),
            recommendations=analysis_data.get('recommendations', []),
            fixed_sql=analysis_data.get('fixed_sql', sql_code),
            analysis_time=analysis_time
        )
        
        print(f"✓ Analisi completata in {analysis_time:.1f}s")
        print(f"✓ Problemi trovati: {len(issues)}")
        
        return result
    
    def _build_analysis_prompt(
        self,
        sql_code: str,
        brb_context: str,
        quadratura_context: str,
        etl_name: str
    ) -> str:
        """Costruisce il prompt per Claude"""
        
        prompt = f"""Sei un esperto analista di codice SQL per ETL. Il tuo compito è analizzare il codice SQL di un ETL, confrontarlo con i requisiti di business (BRB) e i risultati di una quadratura (confronto tra vecchio e nuovo workflow), e identificare problemi e proporre correzioni.

=== ETL DA ANALIZZARE ===
Nome: {etl_name}

=== CODICE SQL ===
```sql
{sql_code}
```

{brb_context}

{quadratura_context}

=== TUO COMPITO ===

1. Analizza il codice SQL confrontandolo con le regole di business (BRB)
2. Identifica le cause dei problemi rilevati nella quadratura
3. Per ogni problema trovato, specifica:
   - Gravità (CRITICAL, HIGH, MEDIUM, LOW)
   - Titolo breve
   - Descrizione dettagliata
   - Regola BRB violata (se applicabile)
   - Numero di riga approssimativo nel codice
   - Codice attuale problematico
   - Codice corretto proposto
   - Spiegazione della modifica

4. Crea anche una versione corretta completa del codice SQL

Rispondi SOLO con un oggetto JSON valido in questo formato (senza markdown, solo JSON puro):

{{
  "issues": [
    {{
      "severity": "CRITICAL|HIGH|MEDIUM|LOW",
      "title": "Titolo breve problema",
      "description": "Descrizione dettagliata",
      "rule_violated": "ID regola BRB (es. RB-004)",
      "line_number": numero_riga_approssimativo,
      "current_code": "codice attuale problematico",
      "proposed_fix": "codice corretto proposto",
      "explanation": "spiegazione della modifica"
    }}
  ],
  "compliance_summary": {{
    "total_rules": numero_totale_regole,
    "compliant": numero_regole_rispettate,
    "violations": numero_violazioni
  }},
  "recommendations": [
    "Raccomandazione generale 1",
    "Raccomandazione generale 2"
  ],
  "fixed_sql": "codice SQL completo corretto con tutte le modifiche applicate"
}}

Concentrati sui problemi che causano le differenze nella quadratura e sulle violazioni delle regole di business più critiche."""

        return prompt
    
    def _parse_response(self, response_text: str) -> Dict:
        """Parse della risposta JSON da Claude"""
        
        try:
            # Rimuovi eventuali markdown code blocks
            cleaned = response_text.strip()
            if cleaned.startswith('```'):
                # Rimuovi ```json e ``` finali
                lines = cleaned.split('\n')
                cleaned = '\n'.join(lines[1:-1])
            
            # Parse JSON
            return json.loads(cleaned)
        
        except json.JSONDecodeError as e:
            print(f"⚠️ Errore nel parsing JSON: {e}")
            print(f"Risposta ricevuta:\n{response_text[:500]}...")
            
            # Fallback: crea struttura base
            return {
                'issues': [],
                'compliance_summary': {
                    'total_rules': 0,
                    'compliant': 0,
                    'violations': 0
                },
                'recommendations': [
                    "Impossibile analizzare completamente il codice. Verifica manualmente."
                ],
                'fixed_sql': ''
            }


# Test del modulo
if __name__ == "__main__":
    import sys
    from storage_reader import read_etl_files
    from excel_analyzer import ExcelAnalyzer
    from dotenv import load_dotenv
    
    # Carica variabili d'ambiente
    load_dotenv()
    
    print("\n🧪 Test CodeAnalyzer (Claude Direct API)\n")
    
    api_key = os.getenv('ANTHROPIC_API_KEY')
    
    if not api_key:
        print("❌ Errore: ANTHROPIC_API_KEY non trovato in .env")
        sys.exit(1)
    
    etl_name = "etl_vendite"
    print(f"Test analisi completa per: {etl_name}\n")
    
    try:
        # 1. Leggi file
        print("📂 Lettura file...")
        data = read_etl_files(etl_name)
        
        # 2. Analizza Excel
        print("\n📊 Analisi Excel...")
        quadratura_analysis = ExcelAnalyzer.analyze_quadratura(data['quadratura'])
        brb_analysis = ExcelAnalyzer.analyze_brb(data['brb'])
        
        # 3. Formatta per AI
        print("\n📝 Preparazione contesto per AI...")
        ai_context = ExcelAnalyzer.format_for_ai(quadratura_analysis, brb_analysis)
        
        # 4. Analizza codice con AI
        print(f"\n🤖 Analisi codice SQL con Claude...")
        analyzer = CodeAnalyzer(api_key=api_key)
        
        result = analyzer.analyze_sql(
            sql_code=data['sql'],
            brb_context=ai_context.split("=== RISULTATI QUADRATURA ===")[0],
            quadratura_context="=== RISULTATI QUADRATURA ===" + ai_context.split("=== RISULTATI QUADRATURA ===")[1],
            etl_name=etl_name
        )
        
        # 5. Mostra risultati
        print(f"\n{'='*60}")
        print(f"RISULTATI ANALISI")
        print(f"{'='*60}")
        print(f"\n⏱️  Tempo analisi: {result.analysis_time:.1f}s")
        print(f"🔍 Problemi trovati: {len(result.issues)}")
        
        if result.issues:
            print(f"\n{'='*60}")
            print("PROBLEMI IDENTIFICATI:")
            print(f"{'='*60}\n")
            
            for i, issue in enumerate(result.issues, 1):
                severity_emoji = {
                    'CRITICAL': '🔴',
                    'HIGH': '🟠',
                    'MEDIUM': '🟡',
                    'LOW': '🟢'
                }.get(issue.severity, '⚪')
                
                print(f"{severity_emoji} {i}. [{issue.severity}] {issue.title}")
                print(f"   Regola violata: {issue.rule_violated}")
                print(f"   Linea ~{issue.line_number}")
                print(f"   Problema: {issue.description}")
                print(f"   Fix proposto: {issue.proposed_fix[:100]}...")
                print()
        
        print(f"\n📊 Conformità:")
        comp = result.compliance_summary
        print(f"   Regole totali: {comp.get('total_rules', 0)}")
        print(f"   Rispettate: {comp.get('compliant', 0)}")
        print(f"   Violazioni: {comp.get('violations', 0)}")
        
        if result.recommendations:
            print(f"\n💡 Raccomandazioni:")
            for rec in result.recommendations:
                print(f"   • {rec}")
        
        print("\n✅ Test completato con successo!")
        
    except Exception as e:
        print(f"❌ Errore: {e}")
        import traceback
        traceback.print_exc()