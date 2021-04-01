# Importer les librairies nécessaires
try:
    import json
    import collections
    import sys
    print("All modules are ok ......")

except Exception as e:
        print("Error  {} ".format(e))

def top_journal(argv):
    try :
        # Récupérer le chemin du fichier JSON
        json_filepath = argv[0]
        
        # Lecture du fichier JSON
        with open(json_filepath) as f:
            data = json.load(f)

        # Récupérer les journaux qui mentionne chaque médicament
        journals_list = []
        for row in data:
            mentions = row["mentioned_in"]
            if "journal" in mentions : 
                journals_list += [mentions["journal"][i]['journal_title'] for i in range(len(mentions["journal"]))]
        
        # Compter le nombre d'occurence de chaque journal et récupérer le top
        top_journal = collections.Counter(journals_list)

        # Retourner le journal ayant mentionné le max des médicamments avec le nombre de mentions
        return top_journal.most_common(1)[0]
    
    except Exception as e:
        print("Error  {} ".format(e))

if __name__ == "__main__":
    print(top_journal(sys.argv[1:]))
