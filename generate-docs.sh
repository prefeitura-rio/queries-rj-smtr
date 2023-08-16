sudo mkdir /credentials-dev

sudo mkdir /credentials-prod

echo $1 > /credentials-dev/dev.json

echo $1 > /credentials-prod/prod.json

dbt docs generate --profiles-dir .