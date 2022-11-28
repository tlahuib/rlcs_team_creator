select
	date_trunc('month', g."date") as month,
	avg(gp.shots) as avg_game_shots,
	avg(gp.goals) as avg_game_goals,
	avg(gp.saves) as avg_game_saves,
	avg(gp.assists) as avg_game_assists,
	avg(gp.score) as avg_game_score,
	sum(case
		when gp.color = 'orange' then g.orange_winner::int
		when gp.color = 'blue' then g.blue_winner::int
	end)::float / count(*)::float as avg_win_rate,
	count(*) as monthly_games
from rocket_league.games_players gp
left join rocket_league.games g 
	on g.id = gp.game_id 
{where}
group by 1
order by 1