alter table public.prices add constraint moment_per_provider unique (moment, provider_id);
