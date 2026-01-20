#!/bin/bash

# Deploy Streamlit Web App (compatibile con tutte le versioni gcloud)

set -e

PROJECT_ID="phrasal-method-484415-g7"
REGION="europe-west1"
SERVICE_NAME="etl-agent-web"
IMAGE_NAME="gcr.io/$PROJECT_ID/$SERVICE_NAME"

echo "🚀 Deploy Streamlit Web App"
echo "============================"
echo ""

# Verifica file
if [ ! -f "streamlit_app.py" ] || [ ! -f "Dockerfile.streamlit" ]; then
    echo "❌ File mancanti!"
    exit 1
fi

echo "✅ File verificati"
echo ""

# Salva Dockerfile originale se esiste
if [ -f "Dockerfile" ]; then
    echo "💾 Backup Dockerfile esistente..."
    cp Dockerfile Dockerfile.backup
fi

# Usa Dockerfile.streamlit come Dockerfile temporaneo
echo "📝 Preparo Dockerfile..."
cp Dockerfile.streamlit Dockerfile

echo ""
echo "🏗️  Building immagine..."
gcloud builds submit --tag $IMAGE_NAME --project $PROJECT_ID

# Ripristina Dockerfile originale
if [ -f "Dockerfile.backup" ]; then
    mv Dockerfile.backup Dockerfile
else
    rm Dockerfile
fi

echo ""
echo "🌐 Deploy su Cloud Run..."
gcloud run deploy $SERVICE_NAME \
    --image $IMAGE_NAME \
    --platform managed \
    --region $REGION \
    --allow-unauthenticated \
    --memory 512Mi \
    --cpu 1 \
    --port 8501 \
    --max-instances 5 \
    --project $PROJECT_ID

echo ""
echo "✅ Deploy completato!"
echo ""

# Ottieni URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
    --region $REGION \
    --project $PROJECT_ID \
    --format 'value(status.url)')

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "🎉 Web App disponibile su:"
echo ""
echo "   $SERVICE_URL"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
