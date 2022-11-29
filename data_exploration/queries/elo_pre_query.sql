-- melt to add win/lose col for better team control
with game_teams as (
	select
		gp.game_id,
		gp.team_id,
		array_agg(gp.id) as players
	from rocket_league.games_players gp
	where
		not coalesce((gp.coach or gp.substitute), false)
		and team_id is not null
	group by 1, 2
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
where
	e.mode = 3
	and not (g.blue_winner is null and g.orange_winner is null)
	and not g.date is null
order by 
	g.date





