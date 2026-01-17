"""
Excel Analyzer - Estrae informazioni utili da BRB e Quadrature
"""

import pandas as pd
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class QuadraturaAnalysis:
    """Risultato analisi quadratura"""
    match_percentage: float
    total_old: int
    total_new: int
    records_match: int
    records_only_old: int
    records_only_new: int
    different_fields: List[Dict]
    squadrature: List[Dict]
    summary_text: str

@dataclass
class BRBAnalysis:
    """Risultato analisi BRB"""
    etl_name: str
    version: str
    owner: str
    frequency: str
    objective: str
    business_rules: List[Dict]
    kpis: List[Dict]
    sources: List[Dict]
    target_fields: List[Dict]


class ExcelAnalyzer:
    """Analizza file Excel BRB e Quadratura"""
    
    @staticmethod
    def analyze_quadratura(quadratura_data: Dict[str, pd.DataFrame]) -> QuadraturaAnalysis:
        """
        Analizza Excel di quadratura
        
        Args:
            quadratura_data: Dict con fogli Excel (da storage_reader)
            
        Returns:
            QuadraturaAnalysis con tutte le info estratte
        """
        
        # Estrai Summary
        summary_df = quadratura_data.get('Summary', pd.DataFrame())
        
        # Parse metriche dal summary
        def get_metric(df, metric_name):
            """Helper per estrarre valore da summary"""
            try:
                row = df[df.iloc[:, 0].str.contains(metric_name, case=False, na=False)]
                if not row.empty:
                    value = str(row.iloc[0, 1])
                    return value
                return None
            except:
                return None
        
        total_old = get_metric(summary_df, "TOTALE.*VECCHIO")
        total_new = get_metric(summary_df, "TOTALE.*NUOVO")
        records_match = get_metric(summary_df, "RECORD IN MATCH")
        records_only_old = get_metric(summary_df, "SOLO IN VECCHIO")
        records_only_new = get_metric(summary_df, "SOLO IN NUOVO")
        match_pct = get_metric(summary_df, "PERCENTUALE MATCH")
        
        # Converti in numeri
        def parse_number(val):
            if val:
                # Rimuovi punti e virgole, prendi solo numeri
                return int(''.join(filter(str.isdigit, str(val))))
            return 0
        
        def parse_percentage(val):
            if val and '%' in str(val):
                return float(str(val).replace('%', '').strip())
            return 0.0
        
        # Estrai campi differenti
        different_fields = []
        if 'Campi Differenti' in quadratura_data:
            diff_df = quadratura_data['Campi Differenti']
            for _, row in diff_df.iterrows():
                if pd.notna(row.iloc[0]) and str(row.iloc[0]) not in ['ID Vendita', 'ID Ordine', 'Campo']:
                    different_fields.append({
                        'id': str(row.iloc[0]),
                        'campo': str(row.iloc[1]) if len(row) > 1 else '',
                        'valore_vecchio': str(row.iloc[2]) if len(row) > 2 else '',
                        'valore_nuovo': str(row.iloc[3]) if len(row) > 3 else '',
                        'tipo_differenza': str(row.iloc[4]) if len(row) > 4 else ''
                    })
        
        # Estrai squadrature
        squadrature = []
        if 'Squadrature' in quadratura_data:
            squad_df = quadratura_data['Squadrature']
            for _, row in squad_df.iterrows():
                if pd.notna(row.iloc[0]) and str(row.iloc[0]) not in ['Tipo']:
                    squadrature.append({
                        'tipo': str(row.iloc[0]),
                        'id': str(row.iloc[1]) if len(row) > 1 else '',
                        'descrizione': str(row.iloc[2]) if len(row) > 2 else ''
                    })
        
        # Crea summary testuale
        summary_text = f"""
QUADRATURA SUMMARY:
- Match: {match_pct} ({records_match} record su {total_old})
- Solo in vecchio workflow: {records_only_old} record
- Solo in nuovo workflow: {records_only_new} record
- Campi con differenze: {len(different_fields)} differenze trovate
- Squadrature totali: {len(squadrature)}
"""
        
        return QuadraturaAnalysis(
            match_percentage=parse_percentage(match_pct),
            total_old=parse_number(total_old),
            total_new=parse_number(total_new),
            records_match=parse_number(records_match),
            records_only_old=parse_number(records_only_old),
            records_only_new=parse_number(records_only_new),
            different_fields=different_fields,
            squadrature=squadrature,
            summary_text=summary_text.strip()
        )
    
    @staticmethod
    def analyze_brb(brb_data: Dict[str, pd.DataFrame]) -> BRBAnalysis:
        """
        Analizza Excel BRB (Business Requirements Baseline)
        
        Args:
            brb_data: Dict con fogli Excel (da storage_reader)
            
        Returns:
            BRBAnalysis con tutte le info estratte
        """
        
        # Estrai Info Generali
        info_df = brb_data.get('Info Generali', pd.DataFrame())
        
        def get_info(df, campo):
            """Helper per estrarre info generali"""
            try:
                row = df[df.iloc[:, 0] == campo]
                if not row.empty:
                    return str(row.iloc[0, 1])
                return ""
            except:
                return ""
        
        etl_name = get_info(info_df, "Nome ETL")
        version = get_info(info_df, "Versione")
        owner = get_info(info_df, "Owner")
        frequency = get_info(info_df, "Frequenza")
        objective = get_info(info_df, "Obiettivo")
        
        # Estrai Regole Business
        business_rules = []
        if 'Regole Business' in brb_data:
            rules_df = brb_data['Regole Business']
            for _, row in rules_df.iterrows():
                if pd.notna(row.iloc[0]) and str(row.iloc[0]) != 'ID Regola':
                    business_rules.append({
                        'id': str(row.iloc[0]),
                        'descrizione': str(row.iloc[1]) if len(row) > 1 else '',
                        'formula': str(row.iloc[2]) if len(row) > 2 else '',
                        'criticita': str(row.iloc[3]) if len(row) > 3 else 'Media'
                    })
        
        # Estrai KPI
        kpis = []
        if 'KPI e Soglie' in brb_data:
            kpi_df = brb_data['KPI e Soglie']
            for _, row in kpi_df.iterrows():
                if pd.notna(row.iloc[0]) and str(row.iloc[0]) != 'ID KPI':
                    kpis.append({
                        'id': str(row.iloc[0]),
                        'descrizione': str(row.iloc[1]) if len(row) > 1 else '',
                        'metrica': str(row.iloc[2]) if len(row) > 2 else '',
                        'soglia': str(row.iloc[3]) if len(row) > 3 else ''
                    })
        
        # Estrai Sorgenti
        sources = []
        if 'Sorgenti' in brb_data:
            src_df = brb_data['Sorgenti']
            for _, row in src_df.iterrows():
                if pd.notna(row.iloc[0]) and str(row.iloc[0]) != 'Tabella':
                    sources.append({
                        'tabella': str(row.iloc[0]),
                        'campo': str(row.iloc[1]) if len(row) > 1 else '',
                        'tipo': str(row.iloc[2]) if len(row) > 2 else '',
                        'descrizione': str(row.iloc[3]) if len(row) > 3 else ''
                    })
        
        # Estrai Target
        target_fields = []
        if 'Target' in brb_data:
            tgt_df = brb_data['Target']
            for _, row in tgt_df.iterrows():
                if pd.notna(row.iloc[0]) and str(row.iloc[0]) != 'Campo':
                    target_fields.append({
                        'campo': str(row.iloc[0]),
                        'tipo': str(row.iloc[1]) if len(row) > 1 else '',
                        'descrizione': str(row.iloc[2]) if len(row) > 2 else '',
                        'derivazione': str(row.iloc[3]) if len(row) > 3 else ''
                    })
        
        return BRBAnalysis(
            etl_name=etl_name,
            version=version,
            owner=owner,
            frequency=frequency,
            objective=objective,
            business_rules=business_rules,
            kpis=kpis,
            sources=sources,
            target_fields=target_fields
        )
    
    @staticmethod
    def format_for_ai(quadratura: QuadraturaAnalysis, brb: BRBAnalysis) -> str:
        """
        Formatta le analisi in un testo leggibile per l'AI
        
        Args:
            quadratura: Analisi della quadratura
            brb: Analisi del BRB
            
        Returns:
            str: Testo formattato per il prompt dell'AI
        """
        
        output = f"""
=== BUSINESS REQUIREMENTS (BRB) ===
ETL: {brb.etl_name}
Versione: {brb.version}
Owner: {brb.owner}
Frequenza: {brb.frequency}
Obiettivo: {brb.objective}

REGOLE DI BUSINESS ({len(brb.business_rules)} regole):
"""
        for rule in brb.business_rules:
            output += f"\n{rule['id']} [{rule['criticita']}]: {rule['descrizione']}"
            if rule['formula']:
                output += f"\n  Formula: {rule['formula']}"
        
        output += f"""

KPI E SOGLIE ({len(brb.kpis)} KPI):
"""
        for kpi in brb.kpis:
            output += f"\n{kpi['id']}: {kpi['descrizione']} - Soglia: {kpi['soglia']}"
        
        output += f"""

=== RISULTATI QUADRATURA ===
{quadratura.summary_text}

PROBLEMI IDENTIFICATI ({len(quadratura.different_fields)} campi con differenze):
"""
        
        # Raggruppa per tipo di differenza
        problems_by_type = {}
        for diff in quadratura.different_fields:
            tipo = diff['tipo_differenza']
            if tipo not in problems_by_type:
                problems_by_type[tipo] = []
            problems_by_type[tipo].append(diff)
        
        for tipo, diffs in problems_by_type.items():
            output += f"\n• {tipo} ({len(diffs)} casi):"
            for diff in diffs[:3]:  # Mostra max 3 esempi per tipo
                output += f"\n  - Campo '{diff['campo']}': '{diff['valore_vecchio']}' → '{diff['valore_nuovo']}'"
        
        if quadratura.squadrature:
            output += f"\n\nSQUADRATURE ({len(quadratura.squadrature)}):"
            for sq in quadratura.squadrature[:5]:  # Max 5
                output += f"\n• {sq['tipo']}: {sq['id']} - {sq['descrizione']}"
        
        return output.strip()


# Test del modulo
if __name__ == "__main__":
    from storage_reader import read_etl_files
    
    print("\n🧪 Test ExcelAnalyzer\n")
    
    # Leggi file ETL
    etl_name = "etl_vendite"
    print(f"Test analisi per: {etl_name}\n")
    
    try:
        data = read_etl_files(etl_name)
        
        print("\n📊 Analisi Quadratura...")
        quadratura_analysis = ExcelAnalyzer.analyze_quadratura(data['quadratura'])
        print(f"✓ Match: {quadratura_analysis.match_percentage}%")
        print(f"✓ Problemi trovati: {len(quadratura_analysis.different_fields)}")
        print(f"✓ Squadrature: {len(quadratura_analysis.squadrature)}")
        
        print("\n📋 Analisi BRB...")
        brb_analysis = ExcelAnalyzer.analyze_brb(data['brb'])
        print(f"✓ ETL: {brb_analysis.etl_name}")
        print(f"✓ Regole business: {len(brb_analysis.business_rules)}")
        print(f"✓ KPI: {len(brb_analysis.kpis)}")
        
        print("\n📝 Formato per AI (primi 500 caratteri):")
        ai_text = ExcelAnalyzer.format_for_ai(quadratura_analysis, brb_analysis)
        print(ai_text[:500] + "...")
        
        print("\n✅ Test completato con successo!")
        
    except Exception as e:
        print(f"❌ Errore: {e}")
        import traceback
        traceback.print_exc()