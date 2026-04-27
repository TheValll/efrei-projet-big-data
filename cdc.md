# Plan du Cahier des Charges — Projet Lakehouse Hubble

## 1. Introduction

1.1 Contexte (Hubble, données ouvertes, enjeux scientifiques)
1.2 Objectifs du projet
1.3 Cadre académique & livrables attendus
1.4 Glossaire & acronymes

## 2. Périmètre fonctionnel

2.1 Les 4 problématiques métier (personas)

- Ingénieur spatial — Suivi de l'usure instrumentale
- Physicien — Cartographie du ciel observé
- Chimiste — Détection de signatures de gaz
- Biologiste — Fiabilité des images vs activité solaire
  2.2 Cas d'usage et questions auxquelles répondre
  2.3 Hors-périmètre

## 3. Sources de données

3.1 Source 1 — MAST (historique, batch, ~500k observations)

- Format, schéma CAOM, volumétrie, fréquence
  3.2 Source 2 — NOAA SWPC (streaming / mini-batch, météo spatiale)
- Endpoints JSON, fréquence (~1 min), volumétrie estimée
  3.3 Justification du choix (deux natures différentes : statique vs flux)

## 4. Architecture cible (Lakehouse Médaillon)

4.1 Vue d'ensemble (schéma Bronze / Silver / Gold)
4.2 Flux de données end-to-end
4.3 Choix technologiques et justifications

- HDFS · YARN · Spark (batch + Structured Streaming) · Hive · Parquet · PostgreSQL · Airflow
  4.4 Topologie / déploiement

## 5. Spécifications fonctionnelles

5.1 Ingestion

- MAST (batch one-shot puis incrémental)
- NOAA SWPC (Spark Structured Streaming)
  5.2 Couche Bronze (raw, immuable)
  5.3 Couche Silver (nettoyage, typage, parsing filtres/s_region, MJD → UTC)
  5.4 Couche Gold — 4 datamarts détaillés
- Modèle de données · règles d'agrégation · SLA fraîcheur
  5.5 Restitution (PowerBI / Superset — bonus)

## 6. Spécifications techniques & non-fonctionnelles

6.1 Volumétrie & performances cibles
6.2 Qualité de données (règles de validation, rejets)
6.3 Orchestration & ordonnancement (DAG Airflow)
6.4 Sécurité, droits d'accès, données publiques
6.5 Monitoring (logs Spark, YARN UI, métriques streaming)
6.6 Reprise sur erreur / idempotence

## 7. Modèle de données détaillé

7.1 Schémas Silver (tables Hive)
7.2 Schémas Gold (tables PostgreSQL des 4 datamarts)
7.3 Dictionnaire de données (renvoi vers data.md)

## 8. Planning & jalons

8.1 Découpage par sprints / lots
8.2 Risques & plan de mitigation

## 9. Livrables

9.1 Code source & dépôt
9.2 Documentation technique
9.3 Captures d'écran exigées (YARN, HDFS, Spark UI, Hive, PostgreSQL)
9.4 Rapport & soutenance

## 10. Annexes

A. Diagrammes (architecture, DAG, MCD/MPD)
B. Exemples de payloads NOAA SWPC
C. Schéma MAST / CAOM (résumé)
D. Références bibliographiques
