# lightdash-playground

This is a playground environment for querying Lightdash data and putting resulting graphs into Google Sheets

Setup: 
Start a postgres instance by running
```
docker run --name motley-postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d pgvector/pgvector:pg17
```

Run initialize.py


