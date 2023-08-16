sudo mkdir ./credentials-dev

sudo mkdir ./credentials-prod

mkdir profiles

echo $1 > ./credentials-dev/dev.json

echo $1 > ./credentials-prod/prod.json

echo """ 
default:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: rj-smtr
      dataset: dbt
      location: US
      threads: 2
      keyfile: ./credentials-dev/dev.json
    prod:
      type: bigquery
      method: service-account
      project: rj-smtr
      dataset: dbt
      location: US
      threads: 2
      keyfile: ./credentials-prod/prod.json""" > profiles/profiles.yml

dbt docs generate --profiles-dir ./profiles