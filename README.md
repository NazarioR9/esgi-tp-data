# TP ESGI IABD 5è année

## Comment construire un jar

Il vous faudra maven 3.3.9 sinon ça ne fonctionnera pas

```bash
mvn package
```

Si vous n'avez pas maven 3.3.9, vous pouvez installer Docker et compiler comme ça :

```bash
DOCKER_BUILDKIT=1 docker build . -o target/<monjar>.jar
```

Votre jar apparaitra dans le dossier `target/`

## Déployer son jar

Pour déployer le jar, je vous recommande de le déplacer dans un dossier sur HDFS comme `/jars/groupe_X`

## Déployer son DAG

Pour déployer son DAG, il faut le copier sur votre edge dans le dossier `/root/airflow/dags`. Nommez bien votre dag avec le nom de votre groupe dedans.
