import time
from multiprocessing import Manager, Process
from MaxPar import Task, TaskSystem  # Importation des classes nécessaires
import threading

# Définition des fonctions des tâches

def run_t1(shared_vars):
    shared_vars["x"] = 2  # Assigne 2 à x

def run_t2(shared_vars):
    shared_vars["y"] = 10  # Assigne 10 à y

def run_tsomme(shared_vars):
    # Vérifie que x et y existent avant d'effectuer la somme
    if shared_vars["x"] is not None and shared_vars["y"] is not None:
        shared_vars["z"] = shared_vars["x"] + shared_vars["y"]  # Additionne x et y et stocke le résultat dans z

def task_runner(task_function, shared_vars):
    task_function(shared_vars)  # Exécute la fonction de la tâche

if __name__ == "__main__":
    # Crée un espace mémoire partagé entre les processus
    with Manager() as manager:
        shared_vars = manager.dict()  # Dictionnaire partagé entre les tâches
        
        # Initialisation des variables partagées avec None (valeurs inconnues au départ)
        shared_vars["x"] = None  
        shared_vars["y"] = None
        shared_vars["z"] = None
        
        # Définition des tâches avec ce qu'elles écrivent et lisent
        t1 = Task(name="T1", writes=["x"], run_function=run_t1)  # Tâche T1 écrit x
        t2 = Task(name="T2", writes=["y"], run_function=run_t2)  # Tâche T2 écrit y
        t_somme = Task(name="somme", writes=["z"], reads=["x", "y"], run_function=run_tsomme)  # Tâche somme lit x et y, puis écrit z
        
        # Définition des dépendances entre les tâches
        dependencies = {
            "T1": [],  # T1 n'a pas de dépendance
            "T2": [],  # T2 non plus
            "somme": []
        }

        # Création du système de gestion des tâches
        system = TaskSystem([t1, t2, t_somme], dependencies)

        # Vérification des cycles dans les dépendances (évite les boucles infinies)
        if system.A_Cycle_avec_Dependance(dependencies):
            exit()  # Si un cycle est détecté, on arrête tout !

        import threading  # Importer le module threading pour les threads

        # Création de la liste pour stocker les threads
        threads = []

        # Création et démarrage des threads
        for task in system.tasks:
            # Remplacer le Process par un Thread
            t = threading.Thread(target=task_runner, args=(task.run_function, shared_vars))  # Utilisation de threads
            threads.append(t)  # Ajouter le thread à la liste
            t.start()  # Démarrer le thread

        # Attente de la fin de tous les threads
        for t in threads:
            t.join()  # Bloque l'exécution jusqu'à ce que le thread se termine

        # Vérification du déterminisme (est-ce que le programme produit toujours le même résultat ?)
        system.detTestRdm(shared_vars)
        
        # Calcul du coût en parallélisme (mesure de l'efficacité d'exécution en parallèle)
        system.parCost(shared_vars, silent=True)
        
        # Génération et affichage du graphe des dépendances des tâches
        system.draw()
