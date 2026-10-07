from collections import deque
from multiprocessing import Process, Manager
import threading
import time
import graphviz
import random

# task runner :
# Exécute chaque tâche dans un thread, dans le cas où une tâche a des dépendances, les dépendances vont s'exécuter naturellement (pas de fonction à part pour sa) dans des threads à part.
# Les tâches n'ayant de dépendances s'exécutent directement sans attendre, parallèlement s'il y a plusieurs tâches dans ce cas
def task_runner(task_name, task_map, dependencies, completed, shared_vars):
    task = task_map[task_name]  # On récupère la tâche par son nom à partir du dictionnaire task_map
    for dep in dependencies[task_name]:  # Pour chaque dépendance de la tâche
        while not completed.get(dep, True):  # True = val par défaut, s'il n'y a pas dep dans completed (ce qu'on veut)
            # Si la dépendance n'est pas encore terminée
            time.sleep(0.001)  # Attendre un petit moment avant de vérifier à nouveau

    task.runT(shared_vars)  # Exécute la fonction associée à la tâche
    completed[task_name] = True  # Marque la tâche comme terminée



class Task:
    def __init__(self, name, reads=None, writes=None, run_function=None):
        self.name = name
        self.reads = reads or []
        self.writes = writes or []
        self.run_function = run_function  # f que va exec runt1 , T2 etc


    def runT(self, shared_vars,silent = False):
        if self.run_function:  # check si elle a bien était défini ( no = None )
            time.sleep(0.01)
            self.run_function(shared_vars)  # Exécute la fonction associée à la tâche avec shared_vars
            #print(" cas c ok ")
        else:
            if not silent:
                print(f"Soucis avec la tâche {self.name}")

class TaskSystem:
    def __init__(self, tasks, precedence):
        self.tasks = tasks  # Liste des tâches à exécuter
        self.precedence = precedence  # l'ordre d'exécution (+ précisément, precedence de x, les t avant lui)
        self.task_map = {task.name: task for task in tasks}  # Dictionnaire de mapping des tâches par leur nom
        self.validate_inputs()
        self.dependencies = self.Parallelisme_max(silent=False)  # Construction des dépendances parallèles maximales
        
        print("Dépendances finales:", self.dependencies)
        
        print("------------------------------------------------------------------------------------------------------------------------")
        print("")

    


    # validate_inputs :
    # vérifie certaines conditions pour s'assurer que le systeme de tâche entré est opérationnel (sans compter les conflits de lecture/écriture)
    def validate_inputs(self):

        # condition : vérifie si les noms sont identiques, car si deux tâches ont le même non on ne sait laquel effecute quoi
        names = [task.name for task in self.tasks]  # Liste des noms des tâches
        if len(names) != len(set(names)):  # Vérification si tous les noms sont uniques grâce à set car set élimine les doublons
            raise ValueError("Les noms des tâches doivent être uniques.")
        print("Tous les noms sont unique.")

        # condition : vérifie que toutes les tâches sont bien définies dans le dictionnaire de tâches
        for task_name, deps in self.precedence.items():
            if task_name not in self.task_map:  # Vérifie que toutes les tâches sont dans task_map
                raise ValueError(f"Tâche {task_name} non trouvée.")
            for dep in deps:  # Vérifie que toutes les dépendances existent dans task_map
                if dep not in self.task_map:
                    raise ValueError(f"Dépendance {dep} non trouvée pour {task_name}.")
        print("Toutes les tâches sont présentes.")

        # condition : présence d'un cycle
        if self.A_Cycle_avec_Dependance(self.precedence):
            raise ValueError("Cycle détecté dans les dépendances !")
        print("Pas de cycle.")



        # condition : vérification des variables définies
        all_written_vars = set(var for task in self.task_map.values() for var in task.writes)
        for task in self.task_map.values():
            for var in task.reads:
                if var not in all_written_vars:
                    raise ValueError(f"La tâche {task.name} lit la variable {var} qui n'est jamais écrite !")
        print("Les variables sont bien définies.")

    def Parallelisme_max(self, silent=False):
        # Cette méthode calcule le parallélisme maximal d'après les conditions de Berstein
        dependencies = {task.name: set(self.precedence.get(task.name, [])) for task in self.tasks}

        if not silent:
            print("")
            print("Dépendances initiales (avant Bernstein) :", dependencies)
        # Matrice de dépendances basée sur les lectures/écritures
        for t1 in self.tasks:
            for t2 in self.tasks:
                if t1.name == t2.name:
                    continue
                # Condition de Berstein
                t1_reads_t2_writes = set(t1.reads) & set(t2.writes)
                write_write_conflict = set(t1.writes) & set(t2.writes)

                # Traiter chaque type de conflit séparément

                # Cas 1 : t1 lit ce que t2 écrit → t1 dépend de t2
                if t1_reads_t2_writes:
                    # on crée ce dict pour vérifier si l'ajout des dépendances engendre un cycle afin de ne pas modifier les dépendances originaux
                    temp_dependencies = {k: set(v) for k, v in dependencies.items()}
                    if t2.name in dependencies[t1.name] :
                        if not silent :
                            print (f"La dépendance {t1.name} -> {t2.name} est déjà présente")
                    else :
                        temp_dependencies[t1.name].add(t2.name)
                        if not self.A_Cycle_avec_Dependance(temp_dependencies):
                            dependencies[t1.name].add(t2.name)
                            if not silent:
                                print(f"Ajout de la dépendance {t1.name} -> {t2.name}")
                                print(f"(raison: {t1.name} lit {t1_reads_t2_writes} écrit par {t2.name})")
                                print("-------------------------------------")
                        else:
                            if not silent:
                                print(f"Dépendance {t1.name} -> {t2.name} rejetée (cycle détecté)")

                # Cas 3 : Conflit d'écriture
                if write_write_conflict:
                    temp_dependencies = {k: set(v) for k, v in dependencies.items()}
                    if t2.name in dependencies[t1.name]:
                        if not silent:
                            print(f"La dépendance {t1.name} -> {t2.name} est déjà présente")
                    else:
                        temp_dependencies[t1.name].add(t2.name)
                        if not self.A_Cycle_avec_Dependance(temp_dependencies):
                            dependencies[t1.name].add(t2.name)
                            if not silent:
                                print(f"Ajout de la dépendance {t1.name} -> {t2.name}")
                                print(f"(raison: {t1.name} lit {t1_reads_t2_writes} écrit par {t2.name})")
                                print("-------------------------------------")
                        else:
                            if not silent:
                                print(f"Dépendance {t1.name} -> {t2.name} rejetée (cycle détecté)")

        return dependencies
        # il est mieux d'étudier chaque cas un par un car sinon on risque de tomber sur des cycles si on englobe les conditions de Berstein en une seule variable.

    def A_Cycle_avec_Dependance(self, dependencies):
        # méthode pour vérifier si un système possède un cycle dans les dépendances
        # Cette méthode a été faite de la façon suivante, car pour vérifier chaque dépendance d'une tâche, il est mieux d'utiliser une fonction récursive.

        visited = set()
        stack = set()
        def visit(node):  # vérifie si node est déjà dans stack
            if node in stack: # si node est dans stack ça veut dire qu'il y est deux fois donc y'a un cycle
                return True
            if node in visited: # si node est dans visited ça veut dire que toutes dépendances ont été vérifiés et donc pas de cycle
                return False
            stack.add(node) # ajoute node dans stack pour commencer l'exploration de ses dépendances
            for dep in dependencies.get(node, []):
                if visit(dep):
                    return True
            stack.remove(node) # pas de cycle donc on enlève de stack et on le met dans visited pour dire qu'on a exploré le nœud et ses dépendances
            visited.add(node)
            return False
        return any(visit(task) for task in self.task_map)

    
    
    def getDependence(self, task_name):
        #cette méthode retourne la liste des tâches qui doivent s'exécuter avant task_name selon le parallélisme maximal

        # Cette approche est plus optimal qu'une autre, car elle calcule en avance les dépendances si elles n'existent pas (avec hasattr), ce qui nous offre un gain de temps lors de l'exécution.
        # L'utilisation de la liste est meilleur pour voir l'ordre d'exécution des tâches.

        # Vérifie si `self.precedence` est bien à jour, sinon le recalculer
        if not hasattr(self, "precedence") or not self.precedence:
            self.precedence = self.Parallelisme_max(silent=True)  # Mettre à jour self.precedence

        if task_name not in self.precedence:
            raise ValueError(f"Tâche {task_name} non trouvée.")
        return list(self.precedence[task_name])  # Retourne la liste des tâches nécessaires avant `task_name`

    
    
    
    def runSeq(self,shared_vars,silent = False):
        # cette méthode effectue l'exécution séquentielle, elle aurait pu être fait autrement également, par exemple à l'aide d'une fonction récursive pour exécuter les dépendances.
        # on a choisi cette approche car ça représente bien l'idée visuellement, avec un graphe on pourrait mieux comprendre comment marche cette fonction.
        in_degree = {task: 0 for task in self.dependencies}  # Compteur qui représente le nombre de dépendances qui par tâches
        graph = {task: [] for task in self.dependencies}  # Graph des dépendances

        # ajout des dépendances
        for task, deps in self.precedence.items():  # Construction du graphe
            for dep in deps:
                graph[dep].append(task)  # L'ajout de la dépendance inverse
                in_degree[task] += 1  # Incrémente le compteur d'entrée pour chaque tâche

        # file d'attente contenant les tâches pouvant être exécuté immédiatement car elles sont indépendantes, c'est comme une file d'attente FIFO
        queue = deque([task for task in in_degree if in_degree[task] == 0])  # File des tâches sans dépendance
        order = []  # Liste des tâches dans l'ordre d'exécution
        while queue:
            node = queue.popleft() # récupère le premier élément ajouté dans queue
            order.append(node)
            for neighbor in graph[node]: # pour toutes les tâches dépendant de node, on enlève -1 dans le compteur car elle vient d'être exécutée
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(order) != len(self.tasks):  # Vérification des cycles pendant l'exécution séquentielle, si on a plus de tâches dans ordre d'exécution, c'est qu'il y a deux fois la même tâche donc cycle
            raise RuntimeError("Cycle détecté lors de l'exécution séquentielle.")

        for task_name in order:  # Exécution des tâches dans l'ordre correct
            self.task_map[task_name].runT(shared_vars)  # Ajout de 'shared_vars'

        if not silent:
            print("Toutes les tâches sont terminées (exécution séquentielle).")
    
    
    def run(self, shared_vars, max_parallelism=4, silent = False):
        dependencies = self.Parallelisme_max(silent=True)
        completed_tasks = set()
        task_threads = []

        while len(completed_tasks) < len(self.tasks):
            ready_tasks = [task for task in self.tasks if task.name not in completed_tasks and all(
                dep in completed_tasks for dep in dependencies.get(task.name, []))]

            ready_to_run = ready_tasks[:max_parallelism]

            for task in ready_to_run:
                # Correction ici: ajouter une virgule pour créer un tuple d'un seul élément
                task_thread = threading.Thread(target=task.runT, args=(shared_vars,))
                task_threads.append(task_thread)
                task_thread.start()

            for task_thread in task_threads[-len(ready_to_run):]:
                task_thread.join()

            completed_tasks.update(task.name for task in ready_to_run)

        if not silent:
            print("Toutes les tâches sont terminées.")
            print(f"Tâches complétées : {completed_tasks}")
   
    
    
    
    
    
    def detTestRdm(self, shared_vars):
     # Teste si le système de tâches est déterministe en exécutant plusieurs fois le système avec les mêmes valeurs initiales et en comparant les résultats.
     # On va commencer par récupérer toutes les variables écrites/lues puis fixer des valeurs initiales aléatoirement, on le fait dès le début, car toutes les exécutions doivent avoir les mêmes valeurs d'entrée.
     # On a imposé 15 exécutions, on va créer une copie des variables partagées, on va mettre à jour avec les valeurs initiales calculées. On utilise le update pour éviter qu'il y a des variables non initialisées.
     # On va utiliser les threads pour faire exécuter parallèlement les tâches, car si on utilise notre fontion run cela suivra le parallélisme max et ce n'est pas ce qu'on veut pour cette fonction
     # Enfin, on va ajouter un dictionnaire des variables écrites dans une liste, on va ensuite ajouter les paires de clé-valeurs dans un set pour eliminer les doublons

# 1. Récupération de toutes les variables écrites et lues par les tâches
     all_writes = [write for task in self.tasks for write in task.writes]
     all_reads = [read for task in self.tasks for read in task.reads]

     # 2. Initialisation aléatoire des variables avec des valeurs fixes
     fixed_initial_values = {var: random.randint(0, 10) for var in set(all_writes + all_reads)}
     print("Valeurs initiales fixes :", fixed_initial_values)

     results = []

     # 3. On exécute le système 15 fois avec les mêmes conditions initiales
     for _ in range(15):
         local_vars = shared_vars.copy()
         local_vars.update(fixed_initial_values)

         # 4. Gestion de l'exécution parallèle avec threads
         completed = {}
         threads = []

         def task_adapter(task_name, task_map, dependencies, completed, local_vars):
             task = task_map[task_name]
             task.runT(local_vars)


         for task_name in self.dependencies:
             t = threading.Thread(
                 target=task_adapter,  # Utiliser la fonction adaptateur
                 args=(task_name, self.task_map, self.dependencies, completed, local_vars)
             )
             threads.append(t)
             t.start()

         for t in threads:
             t.join()
         results.append({key: local_vars[key] for key in all_writes})

     # 5. Analyse des résultats
     unique_results = set()

     for res in results:
         # Ajouter chaque résultat (trié) dans le set
         unique_results.add(tuple(sorted(res.items())))

     print("Résultats uniques :", unique_results)
     print("")


     # 6. Détermination du caractère déterministe
     if len(unique_results) > 1:
        print('----------------------------------------------------------------------------------------')
        print("")
        print("Le système est indéterministe")
        print("")
        print('------------------------------------------------------------------------------------------------------------------------')
     else:
        print('----------------------------------------------------------------------------------------')
        print("")

        print("Le système est déterministe")
        print("")
        print('------------------------------------------------------------------------------------------------------------------------')

    def parCost(self, shared_vars, runs=10, silent=False):
        # Dans cette méthode de calcul de temps moyen des exécutions parallèle et séquentielle, on va commencer par créer deux liste pour stocker les temps d'exécutions,
        # Puis on va créer une boucle de 10 exécutions pour parallèle et séquentielle puis on va calculer leur temps grâce à time, on ignore les deux résultats (les plus lents),
        # pour avoir un temps d'exécution le plus correct possible, on va ensuite faire les moyennes de ces temps ajoutés dans nos listes.
        #On a utilisé Silent pour éviter que les fonctions run affiche chacun de leurs résultats, de cette manière on a uniquement les temps d'excécution comme on le voulait
        # Lorsque nous avions abouti la fonction parCost, lors du test il y avait plusieurs doublons de la fonction run, c'est pour cela que nous avons utilisé silent pour gérer l'apparition des print

        execTimes_par = []
        execTimes_seq = []

        # Exécution silencieuse avec agrégation des résultats
        for i in range(runs):
            if not silent:
                print(f'Iteration N° {i + 1}')

            # Mode parallèle
            start_par = time.time()
            self.run(shared_vars, silent=True)  # Appel en mode silencieux
            time_par = time.time() - start_par
            execTimes_par.append(time_par)

            # Mode séquentiel
            start_seq = time.time()
            self.runSeq(shared_vars, silent=True)  # Appel en mode silencieux
            time_seq = time.time() - start_seq
            execTimes_seq.append(time_seq)

            if not silent:
                print(f"Parallèle: {time_par:.4f} sec")
                print(f"Séquentiel: {time_seq:.4f} sec")
                print("\n" + "-" * 120 + "\n")

        # Calcul des statistiques (en ignorant les 2 pires résultats)
        if len(execTimes_par) > 2:
            execTimes_par_sorted = sorted(execTimes_par)[2:]  # Ignorer les 2 pires
            execTimes_seq_sorted = sorted(execTimes_seq)[2:]  # Ignorer les 2 pires
        else:
            execTimes_par_sorted = execTimes_par
            execTimes_seq_sorted = execTimes_seq

        avg_par = sum(execTimes_par_sorted) / len(execTimes_par_sorted)
        avg_seq = sum(execTimes_seq_sorted) / len(execTimes_seq_sorted)

        # Affichage unique des résultats
        print("\nRésultats finaux:")
        print('------------------')
        print(f"- Temps moyen parallèle ({len(execTimes_par_sorted)} runs): {avg_par:.4f} sec")
        print(f"- Temps moyen séquentiel ({len(execTimes_seq_sorted)} runs): {avg_seq:.4f} sec")
        print(f"- Speedup: {avg_seq / avg_par:.2f}x")

        return avg_par, avg_seq

    def draw(self):
            dot = graphviz.Digraph()  # Crée un objet Graphviz pour dessiner le graphe
            for task in self.task_map:  # Ajoute les nœuds pour chaque tâche
                dot.node(task)
            for task, deps in self.dependencies.items():  # Ajoute les arêtes entre les tâches et leurs dépendances
                for dep in deps:
                    dot.edge(dep, task)
            dot.render('graph', format='png', cleanup=True)  # Génère le fichier du graphe au format PNG
            print("")
            print("")
            print("Graphe généré dans 'graph.png'")