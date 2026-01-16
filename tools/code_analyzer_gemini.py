"""
Code Analyzer - Usa Vertex AI + Gemini per analizzare SQL e proporre modifiche
"""

import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
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
    """Analizza codice SQL usando Vertex AI + Gemini"""
    
    def __init__(self, project_id: str, location: str = "us-central1", model_name: str = None):
        """
        Inizializza l'analyzer con Vertex AI
        
        Args:
            project_id: ID progetto GCP
            location: Regione GCP (default: us-central1)
            model_name: Nome del modello Gemini (default: gemini-1.5-pro)
        """
        self.project_id = project_id
        self.location = location
        self.model_name = model_name or "gemini-3-flash-preview"
        
        # Inizializza Vertex AI
        vertexai.init(project=project_id, location=location)
        
        # Inizializza modello Gemini
        self.model = GenerativeModel(self.model_name)
    
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
        
        print(f"🤖 Analisi SQL con Gemini via Vertex AI...")
        
        # Costruisci il prompt per Gemini
        prompt = self._build_analysis_prompt(
            sql_code=sql_code,
            brb_context=brb_context,
            quadratura_context=quadratura_context,
            etl_name=etl_name
        )
        
        # Configurazione per output JSON strutturato
        generation_config = GenerationConfig(
            temperature=0.2,  # Bassa temperatura per output più deterministico
            max_output_tokens=8000,
        )
        
        # Chiama Gemini via Vertex AI
        import time
        start_time = time.time()
        
        response = self.model.generate_content(
            prompt,
            generation_config=generation_config
        )
        
        analysis_time = time.time() - start_time
        
        # Estrai il contenuto della risposta
        response_text = response.text
        
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
        """Costruisce il prompt per Gemini"""
        
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
        """Parse della risposta JSON da Gemini"""
        
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
    
    print("\n🧪 Test CodeAnalyzer (Gemini)\n")
    
    project_id = os.getenv('GCP_PROJECT_ID')
    location = os.getenv('GCP_LOCATION', 'us-central1')
    model_name = os.getenv('MODEL_NAME', 'gemini-1.5-pro')
    
    if not project_id:
        print("❌ Errore: GCP_PROJECT_ID non trovato in .env")
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
        print(f"\n🤖 Analisi codice SQL con Gemini ({model_name})...")
        analyzer = CodeAnalyzer(project_id=project_id, location=location, model_name=model_name)
        
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