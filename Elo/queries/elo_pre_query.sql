-- melt to add win/lose col for better team control
with game_teams as (
	select 
		gp.event_id,
		gp.team_id,
		gp.game_id,
		array_agg(distinct gp.id) as players
	from rocket_league.games_players gp
	left join rocket_league.events e 
		on e.id = gp.event_id
	where
		e.mode = 3
		and not coalesce((gp.coach or gp.substitute), false)
		and gp.team_id is not null
	group by 1, 2, 3
), game_count as (
	select
		gt.event_id,
		gt.team_id,
		gt.players,
		count(game_id) as n_games
	from game_teams gt
	where
		array_length(gt.players, 1) = 3
	group by 1, 2, 3
), game_count_ordered as (
	select
		*,
		row_number() over(partition by event_id, team_id order by n_games desc) as _row
	from game_count gc
), event_count as (
	select distinct
		gco.team_id,
		gco.players,
		count(distinct gco.event_id) n_events
	from game_count_ordered gco
	where
		gco._row = 1
	group by 1, 2
), valid_games as (
	select
		gt.game_id,
		count(*) = 2 is_valid
	from event_count ec
	left join game_teams gt
		on gt.team_id = ec.team_id
		and gt.players = ec.players
	where
		ec.n_events > 1
	group by 1
)
select
	g.date,
	case
		when g.blue_winner then g.blue_team_id
		when g.orange_winner then g.orange_team_id
	end as winner_team_id,
	case
		when g.blue_winner then bgt.players
		when g.orange_winner then ogt.players
	end as winner_team_players,
	case
		when g.blue_winner then g.orange_team_id
		when g.orange_winner then g.blue_team_id
	end as loser_team_id,
	case
		when g.blue_winner then ogt.players
		when g.orange_winner then bgt.players
	end as loser_team_players
from rocket_league.events e 
left join rocket_league.games g
	on e.id = g.event_id
left join game_teams bgt
	on g.id = bgt.game_id 
	and g.blue_team_id = bgt.team_id
left join game_teams ogt
	on g.id = ogt.game_id 
	and g.orange_team_id = ogt.team_id
left join valid_games vg
	on vg.game_id = g.id
where
	not (g.blue_winner is null and g.orange_winner is null)
	and not g.date is null
	and vg.is_valid
order by 
	g.date