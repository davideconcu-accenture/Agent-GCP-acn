"""
ETL Agent Web Interface
Streamlit app per interfacciarsi con l'agent
"""

import streamlit as st
import requests
import json
from datetime import datetime

# Configurazione
SERVICE_URL = "https://etl-agent-gpr6ltjnsq-ew.a.run.app"

# Configurazione pagina
st.set_page_config(
    page_title="ETL Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS Custom
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .stat-box {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .success-box {
        background-color: #d4edda;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
    }
    .error-box {
        background-color: #f8d7da;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #dc3545;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<p class="main-header">🤖 ETL Agent</p>', unsafe_allow_html=True)
st.markdown("**Assistente AI per analisi e troubleshooting ETL**")
st.markdown("---")

# Sidebar - Configurazione
with st.sidebar:
    st.header("⚙️ Configurazione")
    
    # Health check
    with st.spinner("Verifica connessione..."):
        try:
            response = requests.get(f"{SERVICE_URL}/health", timeout=5)
            if response.status_code == 200:
                st.success("✅ Servizio online")
            else:
                st.error("❌ Servizio non disponibile")
        except:
            st.error("❌ Impossibile connettersi")
    
    st.markdown("---")
    
    # Parametri
    st.subheader("Limiti Esecuzione")
    max_cost = st.slider(
        "Budget massimo ($)",
        min_value=0.05,
        max_value=1.0,
        value=0.5,
        step=0.05,
        help="Limite di spesa per singola analisi"
    )
    
    max_time = st.slider(
        "Timeout (secondi)",
        min_value=30,
        max_value=180,
        value=90,
        step=10,
        help="Tempo massimo di esecuzione"
    )
    
    st.markdown("---")
    
    # Statistiche sessione
    if 'stats' not in st.session_state:
        st.session_state.stats = {
            'total_requests': 0,
            'total_cost': 0.0,
            'successful': 0,
            'failed': 0
        }
    
    st.subheader("📊 Statistiche Sessione")
    st.metric("Richieste totali", st.session_state.stats['total_requests'])
    st.metric("Costo totale", f"${st.session_state.stats['total_cost']:.4f}")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Successi", st.session_state.stats['successful'])
    with col2:
        st.metric("Falliti", st.session_state.stats['failed'])
    
    if st.button("🔄 Reset Stats"):
        st.session_state.stats = {
            'total_requests': 0,
            'total_cost': 0.0,
            'successful': 0,
            'failed': 0
        }
        st.rerun()

# Main area - Tabs
tab1, tab2, tab3 = st.tabs(["🔍 Analisi", "📋 Esempi", "📖 Guida"])

# TAB 1: Analisi
with tab1:
    st.header("Richiesta di Analisi")
    
    # Esempi quick-select
    st.subheader("🚀 Template Veloci")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📊 Analizza Quadratura"):
            st.session_state.task_input = "La quadratura di ETL vendite mostra differenze sugli importi netti tra vecchio e nuovo workflow. Alcuni record hanno differenze di pochi centesimi. Analizza la causa e proponi un fix."
    
    with col2:
        if st.button("📝 Lista ETL"):
            st.session_state.task_input = "Lista tutti gli ETL disponibili per l'analisi"
    
    with col3:
        if st.button("🔍 Verifica BRB"):
            st.session_state.task_input = "Verifica che il codice SQL di ETL vendite rispetti tutte le regole di business definite nel BRB"
    
    st.markdown("---")
    
    # Input principale
    task = st.text_area(
        "Descrivi il problema o la richiesta:",
        height=150,
        placeholder="Es: La quadratura mostra 60 record presenti solo nel vecchio workflow di ETL vendite. Investiga la causa...",
        value=st.session_state.get('task_input', ''),
        key='task_area'
    )
    
    # Bottone analisi
    analyze_button = st.button("🚀 Analizza", type="primary", use_container_width=True)
    
    if analyze_button and task:
        with st.spinner("🤖 L'agent sta lavorando..."):
            try:
                # Chiamata API
                response = requests.post(
                    f"{SERVICE_URL}/analyze",
                    json={
                        "task": task,
                        "max_cost": max_cost,
                        "max_time": max_time
                    },
                    timeout=max_time + 10
                )
                
                result = response.json()
                
                # Update stats
                st.session_state.stats['total_requests'] += 1
                
                if result.get('success'):
                    st.session_state.stats['successful'] += 1
                    st.session_state.stats['total_cost'] += result['stats']['cost']
                    
                    # Risultato successo
                    st.markdown('<div class="success-box">', unsafe_allow_html=True)
                    st.success("✅ Analisi completata con successo!")
                    st.markdown('</div>', unsafe_allow_html=True)
                    
                    # Statistiche esecuzione
                    st.subheader("📊 Statistiche Esecuzione")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("💰 Costo", f"${result['stats']['cost']:.4f}")
                    with col2:
                        st.metric("⏱️ Tempo", f"{result['stats']['time']:.1f}s")
                    with col3:
                        st.metric("🔄 Iterazioni", result['stats']['iterations'])
                    with col4:
                        tools_count = sum(result['stats']['tools_used'].values())
                        st.metric("🔧 Tools", tools_count)
                    
                    # Tool usati
                    if result['stats']['tools_used']:
                        with st.expander("🔧 Dettaglio Tools Utilizzati"):
                            for tool, count in result['stats']['tools_used'].items():
                                st.write(f"- **{tool}**: {count}x")
                    
                    # Soluzione
                    st.markdown("---")
                    st.subheader("💡 Soluzione Proposta")
                    st.markdown(result['solution'])
                    
                    # Download soluzione
                    st.download_button(
                        label="📥 Scarica Soluzione",
                        data=result['solution'],
                        file_name=f"soluzione_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
                        mime="text/markdown"
                    )
                    
                else:
                    st.session_state.stats['failed'] += 1
                    st.markdown('<div class="error-box">', unsafe_allow_html=True)
                    st.error(f"❌ Analisi fallita: {result.get('error', 'Errore sconosciuto')}")
                    st.markdown('</div>', unsafe_allow_html=True)
                    
            except requests.exceptions.Timeout:
                st.session_state.stats['failed'] += 1
                st.error("⏱️ Timeout - L'analisi ha richiesto troppo tempo. Prova ad aumentare il timeout o semplificare la richiesta.")
                
            except Exception as e:
                st.session_state.stats['failed'] += 1
                st.error(f"❌ Errore: {str(e)}")
    
    elif analyze_button and not task:
        st.warning("⚠️ Inserisci una descrizione del problema prima di analizzare")

# TAB 2: Esempi
with tab2:
    st.header("📋 Scenari di Esempio")
    
    scenarios = [
        {
            "title": "🔢 Differenze su Importi",
            "description": "Quadratura mostra differenze numeriche",
            "task": "La quadratura di ETL vendite mostra differenze sugli importi netti tra vecchio e nuovo workflow. Alcuni record hanno differenze di pochi centesimi (es: vecchio=150.00, nuovo=149.99). Analizza la causa e proponi un fix.",
            "max_cost": 0.5,
            "max_time": 90
        },
        {
            "title": "📊 Record Mancanti",
            "description": "Record presenti solo in un workflow",
            "task": "La quadratura di ETL vendite mostra 60 record presenti solo nel vecchio workflow. Questo potrebbe indicare perdita di dati. Verifica la causa e proponi una soluzione.",
            "max_cost": 0.5,
            "max_time": 90
        },
        {
            "title": "✏️ Problemi Formattazione",
            "description": "Dati formattati in modo errato",
            "task": "Nel campo cliente_completo di ETL vendite alcuni record hanno spazi doppi tra nome e cognome (es: 'Mario  Rossi'). Proponi un fix SQL rapido.",
            "max_cost": 0.2,
            "max_time": 30
        },
        {
            "title": "⚡ Performance Degradate",
            "description": "ETL troppo lento",
            "task": "ETL vendite impiega 15 minuti invece dei soliti 5 minuti. Non ci sono errori ma è molto più lento del normale. Analizza possibili cause e suggerisci ottimizzazioni.",
            "max_cost": 0.3,
            "max_time": 60
        },
        {
            "title": "📋 Verifica Conformità",
            "description": "Controllo rispetto ai requisiti",
            "task": "Verifica che il codice SQL di ETL vendite rispetti tutte le regole di business definite nel BRB. Identifica eventuali discrepanze.",
            "max_cost": 0.5,
            "max_time": 90
        },
        {
            "title": "📂 Lista ETL",
            "description": "Esplora ETL disponibili",
            "task": "Lista tutti gli ETL disponibili per l'analisi e fornisci una breve descrizione di ciascuno.",
            "max_cost": 0.1,
            "max_time": 30
        }
    ]
    
    for i, scenario in enumerate(scenarios):
        with st.expander(f"{scenario['title']}", expanded=False):
            st.write(f"**Descrizione:** {scenario['description']}")
            st.write(f"**Budget consigliato:** ${scenario['max_cost']}")
            st.write(f"**Timeout consigliato:** {scenario['max_time']}s")
            st.code(scenario['task'], language=None)
            
            if st.button(f"▶️ Usa questo esempio", key=f"use_{i}"):
                st.session_state.task_input = scenario['task']
                st.rerun()

# TAB 3: Guida
with tab3:
    st.header("📖 Guida Utilizzo")
    
    st.markdown("""
    ## Come usare ETL Agent
    
    ### 🎯 Cosa può fare
    
    L'agent può:
    - ✅ Analizzare risultati di quadrature
    - ✅ Identificare differenze tra workflow
    - ✅ Proporre fix per problemi SQL
    - ✅ Verificare conformità ai requisiti (BRB)
    - ✅ Suggerire ottimizzazioni performance
    - ✅ Spiegare il codice ETL
    
    ### 💡 Come scrivere richieste efficaci
    
    #### ✅ Richieste Buone
    ```
    ✓ "La quadratura ETL vendite mostra 15 record con importi_netto 
       differenti di 0.01€. Analizza la causa."
       
    ✓ "ETL ordini: 60 record presenti solo nel vecchio workflow. 
       Verifica perché mancano nel nuovo."
       
    ✓ "Verifica che ETL vendite usi ROUND(x,2) per importi 
       come da regola BRB RB-005"
    ```
    
    #### ❌ Richieste Vaghe
    ```
    ✗ "Analizza ETL"
    ✗ "C'è un problema"
    ✗ "Fixami tutto"
    ```
    
    ### ⚙️ Parametri
    
    - **Budget massimo**: Limite di spesa per l'analisi ($0.05 - $1.00)
      - Task semplici: $0.10 - $0.20
      - Task normali: $0.30 - $0.50
      - Analisi complesse: $0.50 - $1.00
    
    - **Timeout**: Tempo massimo di esecuzione (30s - 180s)
      - Quick fix: 30s
      - Analisi standard: 60-90s
      - Investigazioni profonde: 120-180s
    
    ### 📊 Interpretare i Risultati
    
    L'agent fornisce:
    1. **Causa del problema** (1-2 frasi)
    2. **Soluzione proposta** (codice SQL o azioni)
    3. **Impatto e rischi**
    4. **Raccomandazioni** (opzionale)
    
    ### 🔧 Troubleshooting
    
    **Timeout**
    - Soluzione: Aumenta il timeout o semplifica la richiesta
    
    **Budget Exceeded**
    - Soluzione: Aumenta il budget o dividi in task più piccoli
    
    **L'agent gira a vuoto**
    - Soluzione: Sii più specifico nella richiesta
    
    ### 💰 Costi Tipici
    
    - Lista ETL: ~$0.05
    - Fix formattazione: ~$0.08
    - Analisi quadratura: ~$0.15
    - Verifica completa BRB: ~$0.25
    
    Media: **$0.10 - $0.20** per richiesta
    """)
    
    st.markdown("---")
    st.info("💡 **Tip**: Usa i template veloci nella tab 'Analisi' per iniziare rapidamente!")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 1rem;'>
        🤖 ETL Agent v1.0 | Powered by Claude Sonnet 4 | 
        <a href='https://etl-agent-gpr6ltjnsq-ew.a.run.app' target='_blank'>API Status</a>
    </div>
    """,
    unsafe_allow_html=True
)
