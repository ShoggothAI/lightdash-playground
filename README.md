# lightdash-playground

This is a playground environment for querying Lightdash data and putting resulting graphs into Google Sheets

# Setup with one example table

## Bring up lightdash docker-compose

Start the lightdash containers, including the db 
(https://docs.lightdash.com/self-host/self-host-lightdash-docker-compose)
```
# Clone the Lightdash repo next to this repo
cd ..
git clone https://github.com/lightdash/lightdash && cd lightdash
```

Set the environment variables in lightdash/.env:
```
PGHOST=db
PGPORT=5432
PGUSER=postgres
PGDATABASE=lightdash_data
PGPASSWORD="mysecretpassword"
```

Edit `lightdash/docker-compose.yml` file to add the following line
under db service, so it's visible from the outside:
```yaml
        ports:
            - ${PGPORT:-5432}:${PGPORT:-5432}
```


Start lightdash containers: 
```bash
source .env && docker compose -f docker-compose.yml --env-file .env up --detach --remove-orphans

```

Check that three containers are up with `docker ps -a`

Now you should be able to go to http://localhost:8080 and create an account for yourself.

Don't do anything more in the UI for now, let's set up the backend first

## Set up dbt backend configs

change to lightdash-playground directory
```bash
cd ../lightdash-playground/
dbt init my_dbt_project && cd my_dbt_project
```

Answer its questions like this:
```yaml
my_dbt_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost  # your PG host
      user: postgres # TODO: read from env var
      password: mysecretpassword # TODO: read from env var
      port: 5432
      dbname: lightdash_data # TODO: read from env var
      schema: public    # your schema name
      threads: 1
```
Now go to `~/.dbt/profiles.yml` and add a line `sslmode: disable`, so that the file now looks like

```yaml
my_dbt_project:
  outputs:
    dev:
      dbname: lightdash_data
      host: localhost
      pass: mysecretpassword
      port: 5432
      schema: public
      threads: 1
      type: postgres
      user: postgres
      sslmode: disable
  target: dev
```

Remove the example configs, we'll create a new one with initialize.py

```bash
rm -rf models/example
```

Run `../playground/initialize.py`, it'll create a table with data in the postgres instance, load the data, and create the dbt files

Now run 
```bash
dbt debug
dbt build
dbt run
```

If that all runs without errors, database and dbt have been set up correctly.


## Configure lightdash

```bash
npm install -g @lightdash/cli
```
Make sure you have an up-to-date npm version for this to work.

While still in `my_dbt_project` directory, run
```bash
lightdash login http://localhost:8080
```
Login using the credentials you just created

**Generate Lightdash metadata**

```bash
lightdash dbt run --target dev
```

This will modify the `models/wise_pizza/schema.yml` file to add Lightdash metadata

This is still pretty naive though, can't distinguish between dimensions and metrics. 

So after you've looked at it, let's overwrite it with the one from this repo:
```bash
cp ../configs/schema.yml models/wise_pizza/schema.yml
```

**Create new project**

```bash
lightdash deploy --create
```

Now go to the link that the last command generates, and you'll be inside the project.

For some reason best known to itself, Lightdash won't pick up the correct connection
details for your postgres db, so you'll have to go and fix them in the UI
under "Project settings". Once you've done that, you should be able to access 
the data and draw some charts.

# Accessing lightdash via Python

There appear to be two libraries available, https://github.com/lightdash/python-sdk is nice and official 
but very basic (doesn't even appear to support filters), and https://github.com/yu-iskw/lightdash-client-python/ 
is auto-generated from the OpenAPI docs, so sprawling but appears to be quite comprehensive.

Lightdash API docs: https://docs.lightdash.com/api-reference/v1/introduction

For an example of how to query lightdash via python, see `playground/getting_started.py`