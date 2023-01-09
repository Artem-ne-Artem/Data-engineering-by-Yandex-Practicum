--Создаём представления в сехму analysis
create or replace view de.analysis.orders as select * from de.production.orders;
create or replace view de.analysis.orderitems as select * from de.production.orderitems;
create or replace view de.analysis.orderstatuses as select * from de.production.orderstatuses;
create or replace view de.analysis.orderstatuslog as select * from de.production.orderstatuslog;
create or replace view de.analysis.products as select * from de.production.products;
create or replace view de.analysis.users as select * from de.production.users;