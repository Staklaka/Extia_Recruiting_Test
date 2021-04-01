# Importer les librairies nécessaires
try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd
    import numpy as np
    import re
    import json

    print("All Dag modules are ok ......")

except Exception as e:
        print("Error  {} ".format(e))

def read_data(**kwargs):
    try :
        # Importer le répertoire et le nom du fichier à lire ainsi que le contexte
        DATA_PATH = kwargs.get("DATA_PATH")
        filename = kwargs.get("filename")
        context = kwargs.get("ti")

        # Une fonction pour évaluer les données de type "Date"
        date_parser = lambda x: datetime.strptime(x, "%Y-%d-%m") if '-' in x \
                                else (datetime.strptime(x, "%d %B %Y") if ' ' in x \
                                else datetime.strptime(x, "%d/%m/%Y"))
        # Si fichier des médicaments                     
        if "drugs" in filename :
            df = pd.read_csv(DATA_PATH + filename)
            # Sauvegarder le résultat dans le contexte
            context.xcom_push(key=filename[:-4], value=df)
        else :
            # Si fichier en csv
            if ".csv" in filename :
                df = pd.read_csv(DATA_PATH + filename, parse_dates=['date'], date_parser=date_parser)
                context.xcom_push(key=filename[:-4], value=df)

            # Si fichier en json
            elif ".json" in filename :
                df = pd.read_json(DATA_PATH + filename, convert_dates=True)
                context.xcom_push(key=filename, value=df)
    
    except Exception as e:
        print("Error  {} ".format(e))

def merge_pubmed_data(**context):
    try :
        # Importer les données de "pubmed" du contexte
        df_1 = context.get("ti").xcom_pull(key="pubmed")
        df_2 = context.get("ti").xcom_pull(key="pubmed.json")

        # Concaténer les données en une seule DataFrame
        df_merged = df_1.append(df_2, sort=True, ignore_index=True).sort_values(by=['date'])

        # Sauvegarder le résultat dans le contexte
        context['ti'].xcom_push(key="pubmed_merged", value=df_merged)

    except Exception as e:
        print("Error  {} ".format(e))

def process_data(**context):
    try :
        # Importer les données du contexte
        df_drugs = context.get("ti").xcom_pull(key="drugs")
        df_pubmed = context.get("ti").xcom_pull(key="pubmed_merged")
        df_clinical_trials = context.get("ti").xcom_pull(key="clinical_trials")

        print("HERE DATA DRUGS: ", df_clinical_trials.shape)
        print("HERE DATA PUBMED: ", df_pubmed.shape)
        print("HERE DATA CLINTR: ", df_clinical_trials.shape)

        # Initialisation de la variable pour le résultat en JSON
        result_json = []

        # Boucler sur chaque médicament
        for idx, drug in enumerate(df_drugs['drug']) :
            # Initialiser l'entête du document pour le médicament en cours
            drug_json = {"drug_id" : df_drugs['atccode'][idx], 
                         "drug_name" : drug,
                         "mentioned_in" : {}}
            # Rechercher les indices des lignes des titres dans "pubmed" où le nom du médicament figure
            pubmed_refs = np.where(df_pubmed['title'].str.contains(drug, flags=re.IGNORECASE))[0]
            # Si un résultat est trouvé                                  
            if list(pubmed_refs) :
                # Remplir le document associé à la clé "mentioned_in" par les données des articles de "pubmed"
                drug_json["mentioned_in"]["pubmed"] = [{"article_id" : df_pubmed['id'][ref], 
                                                        "article_title" : df_pubmed['title'][ref],
                                                        "date_mention" : df_pubmed['date'][ref].strftime('%Y-%m-%d')} for ref in pubmed_refs]

                # Remplir le document associé à la clé "mentioned_in" par les données des journaux
                if "journal" in drug_json["mentioned_in"] :
                    drug_json["mentioned_in"]["journal"] += [{"journal_title" : df_pubmed['journal'][ref],
                                                            "date_mention" : df_pubmed['date'][ref].strftime('%Y-%m-%d')} for ref in pubmed_refs]

                else :
                    drug_json["mentioned_in"]["journal"] = [{"journal_title" : df_pubmed['journal'][ref],
                                                            "date_mention" : df_pubmed['date'][ref].strftime('%Y-%m-%d')} for ref in pubmed_refs]

            # Rechercher les indices des lignes des titres dans "clinical_trials" où le nom du médicament figure
            clinical_trials_refs = np.where(df_clinical_trials['scientific_title'].str.contains(drug, flags=re.IGNORECASE))[0]

            # Si un résultat est trouvé
            if list(clinical_trials_refs) :
                # Remplir le document associé à la clé "mentioned_in" par les données des articles de "clinical_trials"
                drug_json["mentioned_in"]["clinical_trials"] = [{"article_id" : df_clinical_trials['id'][ref], 
                                                                "article_title" : df_clinical_trials['scientific_title'][ref],
                                                                "article_date" : df_clinical_trials['date'][ref].strftime('%Y-%m-%d')} for ref in clinical_trials_refs]
                
                # Remplir le document associé à la clé "mentioned_in" par les données des journaux
                if "journal" in drug_json["mentioned_in"] :
                    drug_json["mentioned_in"]["journal"] += [{"journal_title" : df_clinical_trials['journal'][ref],
                                                            "date_mention" : df_clinical_trials['date'][ref].strftime('%Y-%m-%d')} for ref in clinical_trials_refs]

                else :
                    drug_json["mentioned_in"]["journal"] = [{"journal_title" : df_clinical_trials['journal'][ref],
                                                            "date_mention" : df_clinical_trials['date'][ref].strftime('%Y-%m-%d')} for ref in clinical_trials_refs]
            

            # Supprimer les occurences dans la liste des journaux
            if "journal" in drug_json["mentioned_in"] :
                seen = []
                seq = drug_json["mentioned_in"]["journal"]
                drug_json["mentioned_in"]["journal"] = [x for x in seq if x not in seen and not seen.append(x)]

            # Ajouter le document à la liste des résultats
            result_json.append(drug_json)

        # Sauvegarder le résultat dans le contexte
        context['ti'].xcom_push(key="result_json", value=result_json)

    except Exception as e:
        print("Error  {} ".format(e))

def write_result(**kwargs):
    try :
        # Importer le répertoire et le nom du fichier à lire ainsi que le contexte
        DATA_PATH = kwargs.get("DATA_PATH")
        result_filename = kwargs.get("result_json")
        context = kwargs.get("ti")

        # Importer le résultat final du contexte
        result_json = context.xcom_pull(key="result_json")

        # Ecrire le résultat dans un fichier JSON
        with open(DATA_PATH + result_filename, 'w') as json_file:
            json.dump(result_json, json_file, indent=4)
    
    except Exception as e:
        print("Error  {} ".format(e))

# Définition du DAG
with DAG(
        dag_id="TestProject",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 3, 30),
        },
        catchup=False) as f:

    # Lecture du fichier des drugs
    read_drugs_data = PythonOperator(
        task_id="read_drugs_data",
        python_callable=read_data,
        provide_context=True,
        op_kwargs={"filename":"drugs.csv",
                   "DATA_PATH":"./data/"}
    )

    # Lecture du fichier des clinical_trials
    read_clinical_trials_data = PythonOperator(
        task_id="read_clinical_trials_data",
        python_callable=read_data,
        provide_context=True,
        op_kwargs={"filename":"clinical_trials.csv",
                   "DATA_PATH":"./data/"}
    )

    # Lecture du fichier de pubmed en CSV
    read_pubmed_csv_data = PythonOperator(
        task_id="read_pubmed_csv_data",
        python_callable=read_data,
        provide_context=True,
        op_kwargs={"filename":"pubmed.csv",
                   "DATA_PATH":"./data/"}
    )

    # Lecture du fichier de pubmed en JSON 
    read_pubmed_json_data = PythonOperator(
        task_id="read_pubmed_json_data",
        python_callable=read_data,
        provide_context=True,
        op_kwargs={"filename":"pubmed.json",
                   "DATA_PATH":"./data/"}
    )

    # Concatenation des deux fichiers de pubmed (CSV et JSON)
    merge_pubmed_data = PythonOperator(
        task_id="merge_pubmed_data",
        python_callable=merge_pubmed_data,
        provide_context=True,
    )

    # Transformation des données pour générer le graphe de liaison en JSON
    process_data = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        provide_context=True,
    )

    # Ecriture du résultat dans un fichier JSON "result.json"
    write_result = PythonOperator(
        task_id="write_result",
        python_callable=write_result,
        provide_context=True,
        op_kwargs={"result_json":"result.json",
                   "DATA_PATH":"./data/"}
    )

# Règle de dépendences
[read_pubmed_csv_data, read_pubmed_json_data] >> merge_pubmed_data

[read_drugs_data, read_clinical_trials_data, merge_pubmed_data] >> process_data

process_data >> write_result