alter table de.cdm.dm_settlement_report
    add constraint dm_settlement_restaurant_id_unique unique (restaurant_id, settlement_date);