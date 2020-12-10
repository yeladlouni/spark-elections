L'objectif du lab est de traiter les données des elections
que vous pouvez télécharger depuis Kaggle en utilisant Spark.

1. Télécharger le jeux de données des élections aux USA
2. Charger les différentes données sur des dataframe Spark pour baiden et trump
3. Effectuer le nettoyage de données "cleansing"
4. Effectuer le wrangling de données et créer un seul dataframe englobant les données des deux candidats
5. Analyser les données (tweets et retweets) et déduire lequel des candidats a de fortes chances pour gagner
                            

Preprocessing

1. Charger les données des tweets dans un dataframe df
2. Effectuer le split selon les proportions 60%, 20%, 20%
3. Ecrire un RFormula qui contient le nombre de likes, retweets_count et le contenu du tweet
