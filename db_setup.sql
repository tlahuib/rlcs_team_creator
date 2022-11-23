-- This code is written for Postgres

-- Events table
create table rocket_league.events (
	created_at timestamp not null default now(),
	updated_at timestamp not null default now(),
	id varchar(30) not null constraint events_pk primary key,
	slug varchar not null,
	"name" varchar not null,
	start_date timestamp,
	end_date timestamp,
	region varchar(10),
	"mode" smallint,
	prize_amount int,
	prize_currency varchar(10),
	tier varchar(20),
	image varchar,
	"groups" varchar[]
);
	
-- Events stages table
create table rocket_league.events_stages (
	created_at timestamp not null default now(),
	updated_at timestamp not null default now(),
	event_id varchar(30) not null,
	id int not null,
	"name" varchar not null,
	"format" varchar(20),
	region varchar(10),
	start_date timestamp,
	end_date timestamp,
	liquipedia varchar,
	prize_amount int,
	prize_currency varchar(10),
	qualifier bool,
	lan bool,
	venue varchar(100),
	city varchar(50),
	country varchar(10),
	constraint stages_pk
		primary key (event_id, id),
	constraint stages_fk
		foreign key(event_id)
			references rocket_league.events(id)
);


-- Matches table
create table rocket_league.matches (
	
);
