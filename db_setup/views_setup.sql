-- View that gets the first appearance of each team's roster
drop view rocket_league.rosters;
create or replace view rocket_league.rosters as
with grouped_games_players as (
	select
		gp.game_id,
		gp.team_id,
		gp.color,
		string_agg(distinct gp.id, ',' order by gp.id) as team_players,
		string_agg(distinct p.tag, ',' order by p.tag) as team_players_names
	from rocket_league.games_players gp
	left join rocket_league.players p 
		on p.id = gp.id
	group by 1, 2, 3
), games_results as (
	select
		g.date,
		ggp.*,
		case
			when ggp.color = 'blue' and g.blue_winner then 1
			when ggp.color = 'orange' and g.orange_winner then 1
			else 0
		end as "result"
	from grouped_games_players ggp
	left join rocket_league.games g
		on g.id = ggp.game_id
	where
		g.date is not null
), grouped_games_results as (
	select
		gr.team_id,
		gr.team_players,
		gr.team_players_names,
		count(gr.result) as total_games,
		sum(gr.result) as won_games,
		count(gr.result) - sum(gr.result) as lost_games,
		min(gr.date) as first_appearance,
		max(gr.date) as last_appearance
	from games_results gr
	group by 1, 2, 3
)
select 
	ggr.team_id,
	t.name as team_name,
	row_number() over(partition by ggr.team_id order by ggr.first_appearance) - 1 as roster_id,
	ggr.team_players,
	ggr.team_players_names,
	ggr.total_games,
	ggr.won_games,
	ggr.lost_games,
	ggr.first_appearance,
	ggr.last_appearance
from grouped_games_results ggr
left join rocket_league.teams t 
	on t.id = ggr.team_id
order by 
	2, 9


-- View that contains the games in format to calculate Elo ratings
drop materialized view rocket_league.pre_elo_games;
create materialized view rocket_league.pre_elo_games as 
with games_results as (
	select
		coalesce(g.date, m.date + concat(g.number * 5, ' minutes')::interval) as date,
		g.id as game_id,
		case
			when g.blue_winner then coalesce(g.blue_team_id, m.blue_team_id)
			when g.orange_winner then coalesce(g.orange_team_id , m.orange_team_id)
		end as winner_team_id,
		case
			when g.blue_winner then coalesce(g.orange_team_id , m.orange_team_id) 
			when g.orange_winner then coalesce(g.blue_team_id, m.blue_team_id)
		end as loser_team_id
	from rocket_league.games g
	left join rocket_league.events e 
		on e.id = g.event_id
	left join rocket_league.matches m 
		on m.id = g.match_id
	where
		e.mode = 3
		and not(g.orange_winner is null and g.blue_winner is null)
), grouped_games_players as (
	select 
		gp.team_id,
		gp.game_id,
		string_agg(distinct gp.id, ',' order by gp.id) as team_players,
		count(*) as n_players
	from rocket_league.games_players gp
	left join rocket_league.events e 
		on e.id = gp.event_id
	where
		not coalesce((gp.coach or gp.substitute), false)
		and gp.team_id is not null
	group by 1, 2
)
select
	gr.date,
	gr.game_id,
	gr.winner_team_id,
	wt.name as winner_team_name,
	wr.roster_id as winner_roster_id,
	wp.team_players as winner_team_players,
	gr.loser_team_id,
	lt.name as loser_team_name,
	lr.roster_id as loser_roster_id,
	lp.team_players as loser_team_players
from games_results gr
-- Get team players
left join grouped_games_players wp 
	on wp.game_id = gr.game_id
	and wp.team_id = gr.winner_team_id
left join grouped_games_players lp 
	on lp.game_id = gr.game_id
	and lp.team_id = gr.loser_team_id
-- Get team names
left join rocket_league.teams wt 
	on wt.id = gr.winner_team_id
left join rocket_league.teams lt 
	on lt.id = gr.loser_team_id
-- Get team rosters
left join rocket_league.rosters wr
	on wr.team_id = gr.winner_team_id
	and wr.team_players = wp.team_players
left join rocket_league.rosters lr
	on lr.team_id = gr.loser_team_id
	and lr.team_players = lp.team_players
order by 1



	