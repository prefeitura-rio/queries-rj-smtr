mkdir ./credentials-dev

mkdir ./credentials-prod

mkdir ./profiles

bash -c "echo $1 | base64 --decode > ./credentials-dev/dev.json"

bash -c "echo $1 | base64 --decode > ./credentials-prod/prod.json"

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

ls ./profiles

dbt docs generate --profiles-dir ./profiles