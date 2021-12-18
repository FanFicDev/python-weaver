--create extension if not exists pg_trgm;

create domain url as varchar(2048);

create domain oil_timestamp as bigint;
create function oil_timestamp() returns oil_timestamp language sql stable as $$
	select (extract(epoch from current_timestamp) * 1000) :: oil_timestamp;
$$;

create table if not exists encoding (
	id smallserial primary key,
	name varchar(256) not null unique
);

insert into encoding(name)
select name from (select 'utf8' as name) ei
where not exists (select id from encoding e where e.name = ei.name);

insert into encoding(name)
select name from (select 'cp1252' as name) ei
where not exists (select id from encoding e where e.name = ei.name);

create table if not exists web_source (
	id smallserial primary key,
	name varchar(64) not null,
	source varchar(64),
	description varchar(256)
);

create table if not exists web_base (
	id smallserial primary key,
	created oil_timestamp,
	encoding smallint references encoding(id),
	response bytea
);

create table if not exists web (
	id bigserial primary key,
	created oil_timestamp,
	url url not null,
	status smallint not null,
	sourceId smallint references web_source(id),
	encoding smallint references encoding(id),
	response bytea,
	-- probably null
	requestHeaders bytea,
	responseHeaders bytea,
	wbaseId smallint references web_base(id),
	finalUrl url
);

create index if not exists web_idx on web (url, status, created);
create index if not exists web_url_trgm_idx on web using gin (url gin_trgm_ops);

create or replace view web_size as
select w.id, w.created,
	replace(w.url, 'https://www.', '')::url as url,
	w.status,
	ws.name as sourceName, ws.description as sourceDescription,
	e.name as encoding,
	octet_length(w.response) as responseLength,
	octet_length(w.requestHeaders) as requestHeadersLength,
	octet_length(w.responseHeaders) as responseHeadersLength
from web w
left join web_source ws on ws.id = w.sourceId
left join encoding e on e.id = w.encoding
order by w.created desc;

create table if not exists web_queue (
	id bigserial primary key,
	created oil_timestamp,
	url url not null,
	status smallint,
	workerId int4,
	touched oil_timestamp,
	stale oil_timestamp not null,
	musty oil_timestamp not null,
	priority int4
);

create index if not exists web_queue_idx on web_queue (url, status);
create index if not exists web_queue_url_trgm_idx on web_queue using gin (url gin_trgm_ops);

