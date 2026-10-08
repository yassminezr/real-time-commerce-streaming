# Real-Time Commerce Streaming Platform — Architecture

## 1. Business Context

La plateforme simule des événements issus de trois services e-commerce indépendants : Orders, Payments et Inventory.

L'objectif est de reconstruire l'état métier des commandes malgré les événements retardés, hors ordre, dupliqués ou invalides.

## 2. Architecture Overview

Flux principal :

Python Producers → Apache Kafka → Spark Structured Streaming → Apache Cassandra

- **Python :** génération des événements métier.
- **Kafka :** réception, partitionnement et conservation temporaire des événements.
- **Spark Structured Streaming :** validation, déduplication, corrélation, agrégations et gestion de la reprise.
- **Cassandra :** persistance des états, historiques et métriques.
- **Docker Compose :** exécution reproductible de l'infrastructure locale.

## 3. Event Model

### Domaines et événements

| Service | Événements |
| --- | --- |
| Orders | order_created |
| Payments | payment_authorized, payment_failed |
| Inventory | inventory_reserved, inventory_rejected |

### Event Envelope

Chaque événement comporte :

- event_id
- event_type
- event_time
- schema_version
- order_id
- customer_id
- payload

Le format retenu est JSON, avec schema_version = 1.

Périmètre V1 : une ligne de produit, un résultat de paiement et un résultat de réservation par commande.

## 4. Kafka Design

Quatre topics :

- orders
- payments
- inventory-events
- dead-letter-events

Les trois topics métier disposeront initialement de trois partitions chacun.

La Kafka Key métier est order_id.

Cette stratégie regroupe les événements d'une même commande dans la même partition au sein d'un topic, sous réserve d'une configuration stable du partitionneur.

Elle ne garantit pas l'ordre global entre les topics.

## 5. Stream Processing

Spark Structured Streaming assurera :

- Ingestion des messages Kafka.
- Parsing JSON et validation métier.
- Routage des événements invalides vers la DLQ.
- Gestion de l'event-time et du watermarking.
- Déduplication par event_id selon une politique temporelle définie.
- Corrélation des flux Orders et Payments.
- Intégration des événements Inventory.
- Reconstruction de l'état métier des commandes.
- Calcul des commandes par minute et du taux d'échec des paiements sur cinq minutes.
- Checkpointing des requêtes streaming.

Les conditions exactes des joins, des watermarks et de la gestion des événements tardifs seront fixées durant l'implémentation.

## 6. Cassandra Data Model

### order_state_by_id

Question : quel est l'état actuel d'une commande ?

Partition key : order_id.

### events_by_order

Question : quels événements sont associés à une commande ?

Partition key : order_id.

Clustering keys : event_time, event_id.

### streaming_metrics

Question : quelles valeurs d'indicateurs ont été calculées pour chaque fenêtre ?

Partition key : metric_name.

Clustering keys : window_start, window_end.

Ce schéma est adapté au petit périmètre local du projet. Un volume historique important nécessiterait une stratégie de partitionnement plus élaborée.

## 7. Architecture Decisions

### ADR-001 — Apache Kafka

Décision : Kafka pour le transport et la conservation des événements.

Raison : découplage, rétention configurable, relecture et consommation continue.

Limite : un seul broker local, sans haute disponibilité.

### ADR-002 — Kafka Key = order_id

Décision : partitionner les messages selon leur commande.

Raison : cohérence du partitionnement par entité métier.

Limite : aucune garantie d'ordre global entre topics.

### ADR-003 — JSON

Décision : JSON avec schéma commun et payload spécifique.

Raison : simplicité, lisibilité et intégration Python/Spark.

Limite : validation explicite nécessaire, sans Schema Registry en V1.

### ADR-004 — Spark Structured Streaming

Décision : traitement des événements avec Spark.

Raison : streaming stateful, joins, fenêtres temporelles et checkpointing.

Limite : gestion de l'état, consommation de ressources et complexité de reprise.

### ADR-005 — Event-Time et Watermarking

Décision : utiliser l'heure métier pour les traitements temporels.

Raison : gérer les événements arrivant en retard ou dans le désordre.

Limite : les événements dépassant les contraintes temporelles peuvent ne plus être pris en compte.

### ADR-006 — Apache Cassandra

Décision : persister les résultats dans des tables orientées requêtes.

Raison : pratiquer la modélisation NoSQL, le partitionnement et les upserts.

Limite : cohérence des mises à jour et absence de transaction globale automatique entre les trois tables.

### ADR-007 — Docker Compose

Décision : exécuter l'ensemble en environnement local.

Raison : reproductibilité et réduction de la complexité de déploiement.

Limite : pas de résilience multi-nœuds.

## 8. Reliability Scenarios

Scénarios à implémenter et tester :

- Événements dupliqués.
- Événements invalides.
- Événements retardés.
- Événements hors ordre.
- Arrêt et redémarrage du job Spark.

La présence d'un checkpoint ne constitue pas, à elle seule, une garantie exactly-once pour les écritures Cassandra.

## 9. Scope and Limitations

L'objectif est de démontrer un pipeline streaming local, reproductible, correctement testé et explicable en entretien.

Hors périmètre V1 :

- Kubernetes.
- Infrastructure Kafka multi-brokers.
- Cluster Cassandra multi-nœuds.
- Cloud Azure.
- Monitoring distribué avancé.
- Workflows e-commerce complexes.
- Garanties non vérifiées de haute disponibilité ou d'exactly-once de bout en bout.

**Statut : document de conception. Les fonctionnalités doivent encore être implémentées et validées.**