create table public.providers
(
    id   bigserial primary key,
    name varchar not null
);

create table public.prices
(
    moment      timestamp with time zone       not null,
    price       double precision not null,
    provider_id bigint,
    foreign key (provider_id) references providers (id)
);

select create_hypertable('prices', by_range('moment'));

insert into providers(name)
values ('tibber');
select *
from providers;
