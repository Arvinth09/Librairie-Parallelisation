# Parallélisme maximal d'un système de tâches

## Présentation

Projet réalisé dans le cadre d'un projet universitaire sur le **parallélisme maximal**.

L'objectif est de développer un système capable d'analyser les dépendances entre différentes tâches afin d'identifier automatiquement celles qui peuvent être exécutées en parallèle, tout en respectant les contraintes de dépendance.

## Fonctionnalités

- Validation des tâches et de leurs dépendances
- Détection des cycles
- Calcul du parallélisme maximal à partir des dépendances
- Exécution séquentielle des tâches
- Exécution parallèle avec des threads
- Test du déterminisme
- Comparaison des temps d'exécution
- Calcul du speedup
- Génération du graphe des dépendances avec Graphviz

## Technologies

- Python
- Threading
- Multiprocessing
- Graphviz
- Graphes et dépendances
- Programmation parallèle
- Analyse de performances

## Évaluation des performances

Les performances sont comparées entre les modes séquentiel et parallèle à l'aide du temps d'exécution et du **speedup**.

```text
Speedup = Temps séquentiel / Temps parallèle
```

Le projet permet également de vérifier si l'exécution parallèle reste déterministe lorsque les mêmes conditions initiales sont utilisées.

## Perspectives

- Améliorer l'ordonnancement des tâches
- Optimiser la gestion du parallélisme
- Tester le système avec un nombre plus important de tâches
- Améliorer la gestion des ressources et des dépendances
