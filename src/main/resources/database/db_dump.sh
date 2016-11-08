#!/usr/bin/env bash
mysqldump  --max_allowed_packet=1G --host=127.0.0.1 --user=root -proot --complete-insert=TRUE --port=3306 --default-character-set=utf8 "rna_metastore" "qb_dimension_column_map" "qb_fact_column_map" "qb_fact_rank" "qb_join_chain" "rbac_entity" "rbac_entity_attribute" "rbac_operator" "rbac_value_type" "rbac_role_access" "rbac_user_access" > new_rbac_and_queryBuilder_metadata.sql
