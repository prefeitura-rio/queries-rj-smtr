pip install --no-cache-dir -r requirements.txt

echo $1 > /credentials/dev.json

echo $1 > /credentials/prod.json

dbt docs generate --profiles-dir .