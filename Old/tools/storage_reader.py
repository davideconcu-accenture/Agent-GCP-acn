"""
Storage Reader - Legge file da locale o Cloud Storage
Gestisce SQL, Excel (BRB e Quadrature)
"""

import os
from pathlib import Path
from google.cloud import storage
import pandas as pd
from typing import Dict, Optional, Tuple

class StorageReader:
    """Classe per leggere file da locale o Cloud Storage"""
    
    def __init__(self, mode: str = "local", project_id: Optional[str] = None):
        """
        Inizializza il reader
        
        Args:
            mode: "local" per leggere da data/ oppure "cloud" per Cloud Storage
            project_id: ID progetto GCP (necessario solo per mode="cloud")
        """
        self.mode = mode
        self.project_id = project_id
        
        if mode == "cloud":
            if not project_id:
                raise ValueError("project_id è obbligatorio per mode='cloud'")
            self.storage_client = storage.Client(project=project_id)
        else:
            # Definisci path base per modalità locale
            self.base_path = Path("data")
    
    def read_sql_file(self, etl_name: str) -> Tuple[str, str]:
        """
        Legge il file SQL di un ETL
        
        Args:
            etl_name: Nome dell'ETL (es. "etl_vendite")
            
        Returns:
            Tuple[str, str]: (contenuto_sql, path_file)
        """
        if self.mode == "local":
            file_path = self.base_path / "Codice" / etl_name / f"{etl_name}.sql"
            
            if not file_path.exists():
                raise FileNotFoundError(f"File SQL non trovato: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            return content, str(file_path)
        
        else:  # mode == "cloud"
            # TODO: implementare lettura da Cloud Storage quando necessario
            raise NotImplementedError("Lettura da Cloud Storage non ancora implementata")
    
    def read_brb_excel(self, etl_name: str) -> Tuple[Dict[str, pd.DataFrame], str]:
        """
        Legge il file Excel BRB (Business Requirements Baseline)
        
        Args:
            etl_name: Nome dell'ETL (es. "etl_vendite")
            
        Returns:
            Tuple[Dict, str]: (dizionario con tutti i fogli Excel, path_file)
                              chiavi = nomi fogli, valori = DataFrame
        """
        if self.mode == "local":
            file_path = self.base_path / "BRB" / etl_name / f"brb_{etl_name.replace('etl_', '')}.xlsx"
            
            if not file_path.exists():
                raise FileNotFoundError(f"File BRB non trovato: {file_path}")
            
            # Leggi tutti i fogli Excel
            excel_data = pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
            
            return excel_data, str(file_path)
        
        else:  # mode == "cloud"
            raise NotImplementedError("Lettura da Cloud Storage non ancora implementata")
    
    def read_quadratura_excel(self, etl_name: str, date: Optional[str] = None) -> Tuple[Dict[str, pd.DataFrame], str]:
        """
        Legge il file Excel di quadratura
        
        Args:
            etl_name: Nome dell'ETL (es. "etl_vendite")
            date: Data specifica (es. "2025-01-15"). Se None, prende il più recente
            
        Returns:
            Tuple[Dict, str]: (dizionario con tutti i fogli Excel, path_file)
        """
        if self.mode == "local":
            quadrature_dir = self.base_path / "Quadrature" / etl_name
            
            if not quadrature_dir.exists():
                raise FileNotFoundError(f"Directory quadrature non trovata: {quadrature_dir}")
            
            # Se data specificata, cerca file specifico
            if date:
                file_path = quadrature_dir / f"quadratura_{etl_name.replace('etl_', '')}_{date}.xlsx"
            else:
                # Prendi il file più recente
                excel_files = list(quadrature_dir.glob("quadratura_*.xlsx"))
                if not excel_files:
                    raise FileNotFoundError(f"Nessun file quadratura trovato in {quadrature_dir}")
                file_path = max(excel_files, key=lambda p: p.stat().st_mtime)
            
            if not file_path.exists():
                raise FileNotFoundError(f"File quadratura non trovato: {file_path}")
            
            # Leggi tutti i fogli Excel
            excel_data = pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
            
            return excel_data, str(file_path)
        
        else:  # mode == "cloud"
            raise NotImplementedError("Lettura da Cloud Storage non ancora implementata")
    
    def list_available_etls(self) -> list:
        """
        Lista tutti gli ETL disponibili
        
        Returns:
            list: Lista dei nomi ETL disponibili
        """
        if self.mode == "local":
            codice_dir = self.base_path / "Codice"
            if not codice_dir.exists():
                return []
            
            # Lista tutte le cartelle in Codice/
            etls = [d.name for d in codice_dir.iterdir() if d.is_dir()]
            return sorted(etls)
        
        else:  # mode == "cloud"
            raise NotImplementedError("Listing da Cloud Storage non ancora implementato")


# Funzioni helper per uso rapido

def read_etl_files(etl_name: str, mode: str = "local", project_id: Optional[str] = None) -> Dict:
    """
    Funzione di convenienza per leggere tutti i file di un ETL
    
    Args:
        etl_name: Nome dell'ETL
        mode: "local" o "cloud"
        project_id: ID progetto GCP (solo per cloud)
        
    Returns:
        Dict con chiavi: 'sql', 'brb', 'quadratura', 'paths'
    """
    reader = StorageReader(mode=mode, project_id=project_id)
    
    print(f"📂 Lettura file per ETL: {etl_name}")
    
    # Leggi SQL
    print("  ├─ Lettura SQL...")
    sql_content, sql_path = reader.read_sql_file(etl_name)
    print(f"  │  ✓ {len(sql_content)} caratteri da {sql_path}")
    
    # Leggi BRB
    print("  ├─ Lettura BRB...")
    brb_data, brb_path = reader.read_brb_excel(etl_name)
    print(f"  │  ✓ {len(brb_data)} fogli da {brb_path}")
    
    # Leggi Quadratura
    print("  └─ Lettura Quadratura...")
    quadratura_data, quadratura_path = reader.read_quadratura_excel(etl_name)
    print(f"     ✓ {len(quadratura_data)} fogli da {quadratura_path}")
    
    return {
        'sql': sql_content,
        'brb': brb_data,
        'quadratura': quadratura_data,
        'paths': {
            'sql': sql_path,
            'brb': brb_path,
            'quadratura': quadratura_path
        }
    }


# Test del modulo
if __name__ == "__main__":
    # Test lettura file
    print("\n🧪 Test StorageReader\n")
    
    reader = StorageReader(mode="local")
    
    # Lista ETL disponibili
    etls = reader.list_available_etls()
    print(f"ETL disponibili: {etls}\n")
    
    if etls:
        # Testa lettura primo ETL
        test_etl = etls[0]
        print(f"Test lettura completa per: {test_etl}\n")
        
        try:
            data = read_etl_files(test_etl)
            print("\n✅ Test completato con successo!")
            print(f"\nSQL preview (primi 200 caratteri):\n{data['sql'][:200]}...")
            print(f"\nFogli BRB: {list(data['brb'].keys())}")
            print(f"Fogli Quadratura: {list(data['quadratura'].keys())}")
        except Exception as e:
            print(f"\n❌ Errore: {e}")
    else:
        print("⚠️ Nessun ETL trovato in data/Codice/")