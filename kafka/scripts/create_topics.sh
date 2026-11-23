#!/usr/bin/env bash
# boamp-raw       : annonces brutes, 3 partitions pour paralléliser, clé = idweb
# boamp-validated : annonces validées et normalisées
# boamp-raw-dlq   : annonces rejetées (faible volume -> 1 partition)
set -e
for t in "boamp-raw:3" "boamp-validated:3" "boamp-raw-dlq:1"; do
  docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 \
    --create --if-not-exists --topic "${t%%:*}" --partitions "${t##*:}" --replication-factor 1
done
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
