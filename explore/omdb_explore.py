from scorer import execute_sql
DB='postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require'

queries = [
    ('kind values', 'SELECT kind, COUNT(*) FROM movies GROUP BY kind ORDER BY COUNT(*) DESC'),
    ('vote_average range', 'SELECT MIN(vote_average), MAX(vote_average), AVG(vote_average) FROM movies WHERE vote_average IS NOT NULL AND vote_average > 0'),
    ('votes_count range', 'SELECT MIN(votes_count), MAX(votes_count), AVG(votes_count) FROM movies WHERE votes_count IS NOT NULL AND votes_count > 0'),
    ('budget non-null', 'SELECT COUNT(*) FROM movies WHERE budget IS NOT NULL AND budget > 0'),
    ('revenue non-null', 'SELECT COUNT(*) FROM movies WHERE revenue IS NOT NULL AND revenue > 0'),
    ('runtime range', 'SELECT MIN(runtime), MAX(runtime), AVG(runtime) FROM movies WHERE runtime IS NOT NULL AND runtime > 0'),
    ('date range', 'SELECT MIN(date), MAX(date) FROM movies WHERE date IS NOT NULL'),
    ('parent_id non-null', 'SELECT COUNT(*) FROM movies WHERE parent_id IS NOT NULL'),
    ('series_id non-null', 'SELECT COUNT(*) FROM movies WHERE series_id IS NOT NULL'),
    ('homepage non-null', "SELECT COUNT(*) FROM movies WHERE homepage IS NOT NULL AND homepage != ''"),
]
for label, q in queries:
    r = execute_sql(DB, q)
    if r: print(f'{label}: {r[1]}')
