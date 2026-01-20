#!/bin/bash

# Lancia Streamlit App per ETL Agent

echo "🚀 Avvio ETL Agent Web Interface"
echo "================================"
echo ""

# Verifica che streamlit sia installato
if ! command -v streamlit &> /dev/null; then
    echo "📦 Streamlit non trovato. Installazione in corso..."
    
    # Prova pip3 prima, poi pip
    if command -v pip3 &> /dev/null; then
        pip3 install -r requirements-streamlit.txt
    elif command -v pip &> /dev/null; then
        pip install -r requirements-streamlit.txt
    else
        echo "❌ Errore: né pip né pip3 trovati!"
        echo "Installa manualmente con:"
        echo "  python3 -m pip install -r requirements-streamlit.txt"
        exit 1
    fi
fi

echo "🌐 Avvio server Streamlit..."
echo ""
echo "📍 La web app sarà disponibile su:"
echo "   http://localhost:8501"
echo ""
echo "⏹️  Per fermare: Ctrl+C"
echo ""

# Avvia streamlit
if command -v streamlit &> /dev/null; then
    streamlit run streamlit_app.py
else
    # Fallback: usa python3 -m streamlit
    python3 -m streamlit run streamlit_app.py
fi
