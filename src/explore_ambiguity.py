from scorer import execute_sql
DB='postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require'

queries = [
    # === AMBIGUITY 1: "role" in casts — character name, not job ===
    # casts.role is the CHARACTER NAME (e.g. 'Luke Skywalker'), not the job role
    # If you ask "how many distinct roles", do you mean characters (casts.role) or jobs (jobs.name via casts.job_id)?
    ('distinct casts.role values', "SELECT COUNT(DISTINCT role) FROM casts WHERE role IS NOT NULL AND role != ''"),
    ('distinct job types', "SELECT COUNT(DISTINCT job_id) FROM casts"),
    ('sample casts.role', "SELECT role FROM casts WHERE role IS NOT NULL AND role != '' LIMIT 10"),
    
    # === AMBIGUITY 2: movies.name vs movie_aliases_iso.name — which is "the title"? ===
    # If someone asks "how many movies have a name starting with A", which table?
    # movies.name is canonical, movie_aliases_iso.name is translated titles
    ('movies starting A', "SELECT COUNT(*) FROM movies WHERE name LIKE 'A%' AND kind = 'movie'"),
    ('aliases starting A', "SELECT COUNT(*) FROM movie_aliases_iso WHERE name LIKE 'A%'"),
    
    # === AMBIGUITY 3: categories.name vs category_names.name ===
    # categories has a name column AND there's a separate category_names table
    # "What are the genre names?" — which table?
    ('category count', "SELECT COUNT(*) FROM categories"),
    ('category_names count', "SELECT COUNT(*) FROM category_names"),
    ('same check', "SELECT c.name, cn.name, cn.language FROM categories c JOIN category_names cn ON c.id = cn.category_id WHERE c.root_id = 1 LIMIT 10"),
    
    # === AMBIGUITY 4: jobs.name vs job_names.name ===
    # Same pattern. jobs.name is canonical, job_names has translations.
    # BUT: jobs.name might be German! (e.g., Drehbuch)
    ('jobs canonical sample', "SELECT id, name FROM jobs LIMIT 10"),
    ('job_names en sample', "SELECT job_id, name, language FROM job_names WHERE language = 'en' LIMIT 10"),
    
    # === AMBIGUITY 5: movies table contains non-movies ===
    # "How many movies are in the database?" COUNT(*) FROM movies = 211K, but kind='movie' = 60K
    ('all movies table', "SELECT COUNT(*) FROM movies"),
    ('just kind movie', "SELECT COUNT(*) FROM movies WHERE kind = 'movie'"),
    
    # === AMBIGUITY 6: parent_id vs series_id for "what series is an episode from?" ===
    # parent_id → season (intermediate), series_id → series (direct)
    # "Find the series of an episode" — parent_id gives the SEASON, not the series
    ('episode parent is season', "SELECT e.name, e.parent_id, p.name as parent_name, p.kind as parent_kind, e.series_id FROM movies e JOIN movies p ON e.parent_id = p.id WHERE e.kind = 'episode' LIMIT 5"),
    
    # === AMBIGUITY 7: movie_categories vs movie_keywords — both reference categories ===
    # "How many categories are associated with movies?" — which junction table?
    ('movie_categories distinct cats', "SELECT COUNT(DISTINCT category_id) FROM movie_categories"),
    ('movie_keywords distinct cats', "SELECT COUNT(DISTINCT category_id) FROM movie_keywords"),
    
    # === AMBIGUITY 8: official_translation in movie_aliases_iso ===
    # official_translation is an INTEGER (0/1), not boolean or text
    # "How many official translations exist?" 
    ('official translations', "SELECT COUNT(*) FROM movie_aliases_iso WHERE official_translation = 1"),
    ('unofficial translations', "SELECT COUNT(*) FROM movie_aliases_iso WHERE official_translation = 0"),
    
    # === AMBIGUITY 9: "type" column in movie_references ===
    # movie_references.type contains values like 'Remake', 'Influence', 'SpinOff', 'Parody', 'Homage'
    # If you ask "what types of movies exist", you might look at movies.kind or movie_references.type
    ('ref types', "SELECT type, COUNT(*) FROM movie_references GROUP BY type ORDER BY COUNT(*) DESC"),
    ('movie kinds', "SELECT kind, COUNT(*) FROM movies GROUP BY kind ORDER BY COUNT(*) DESC"),
]
for label, q in queries:
    r = execute_sql(DB, q)
    if r: 
        print(f'\n=== {label} ===')
        for row in r[1][:10]: print(row)
