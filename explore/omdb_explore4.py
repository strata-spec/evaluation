from scorer import execute_sql
DB='postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require'

queries = [
    # What job names exist in English?
    ('director job', "SELECT id, name FROM jobs WHERE LOWER(name) LIKE '%direct%'"),
    ('actor job', "SELECT id, name FROM jobs WHERE LOWER(name) LIKE '%act%'"),
    ('writer job', "SELECT id, name FROM jobs WHERE LOWER(name) LIKE '%writ%'"),
    ('producer job', "SELECT id, name FROM jobs WHERE LOWER(name) LIKE '%produc%'"),
    ('top job by cast count', "SELECT j.id, j.name, COUNT(*) as cnt FROM casts c JOIN jobs j ON c.job_id = j.id GROUP BY j.id, j.name ORDER BY cnt DESC LIMIT 15"),
    
    # Genre specific categories
    ('genre categories', "SELECT c.id, c.name FROM categories c WHERE c.root_id = 1 AND c.parent_id = 1 ORDER BY c.name"),
    ('drama count', "SELECT COUNT(*) FROM movie_categories mc JOIN categories c ON mc.category_id = c.id WHERE c.name = 'Drama'"),
    
    # How does parent_id work for episodes/seasons?
    ('episode-season-series chain', """
        SELECT e.id as ep_id, e.name as ep_name, e.parent_id as season_id, 
               s.name as season_name, s.parent_id as series_id_via_parent, 
               e.series_id as series_id_direct
        FROM movies e 
        JOIN movies s ON e.parent_id = s.id 
        WHERE e.kind = 'episode' AND s.kind = 'season'
        LIMIT 5
    """),
    
    # Check if movie_keywords count is reasonable
    ('keyword count', "SELECT COUNT(*) FROM movie_keywords"),
    
    # People with birthday - how many?
    ('birthday count', "SELECT COUNT(*) FROM people WHERE birthday IS NOT NULL"),
    
    # movies with both budget and revenue > 0
    ('profit movies', "SELECT COUNT(*) FROM movies WHERE budget > 0 AND revenue > 0"),
    
    # Check casts_view row count
    ('casts_view count', "SELECT COUNT(*) FROM casts_view LIMIT 1"),
    
    # Check if access_log is empty
    ('access_log count', "SELECT COUNT(*) FROM access_log"),
    
    # Distinct languages count
    ('distinct movie languages', "SELECT COUNT(DISTINCT language) FROM movie_languages"),
    ('distinct category langs', "SELECT COUNT(DISTINCT language) FROM category_names"),
    
    # movie_links check
    ('imdb links', "SELECT COUNT(*) FROM movie_links WHERE source = 'imdbmovie'"),
    
    # Are there movies with votes_count but no vote_average or vice versa?
    ('votes no avg', "SELECT COUNT(*) FROM movies WHERE votes_count > 0 AND (vote_average IS NULL OR vote_average = 0)"),
    ('avg no votes', "SELECT COUNT(*) FROM movies WHERE vote_average > 0 AND (votes_count IS NULL OR votes_count = 0)"),
    
    # How many movies per kind have vote data?
    ('votes by kind', "SELECT kind, COUNT(*) FROM movies WHERE votes_count > 0 GROUP BY kind ORDER BY COUNT(*) DESC"),
]
for label, q in queries:
    r = execute_sql(DB, q)
    if r: 
        print(f'\n=== {label} ===')
        print(f'cols: {r[0]}')
        for row in r[1][:20]: print(row)
