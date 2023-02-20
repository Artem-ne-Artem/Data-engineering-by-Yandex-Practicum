drop table if exists dialogs;

create table dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6),
    message_from int REFERENCES members(id),
    message_to int REFERENCES members(id),
    message varchar(1000),
    message_type varchar(100)
)
order by ХХХ, XXX
SEGMENTED BY hash(message_id) all nodes
PARTITION BY message_ts::date
;