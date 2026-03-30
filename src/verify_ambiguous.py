from scorer import execute_sql
DB='postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require'

# Verify candidate reference SQLs for new ambiguous questions

sqls = {
    # Q006: "How many distinct character names have been performed in cast credits?"
    # Correct: casts.role (character names like 'Luke Skywalker')
    # Trap: COUNT(DISTINCT job_id) from casts (571 job types) or COUNT from jobs (589)
    # or casts.position  
    "q006_correct": "SELECT COUNT(DISTINCT role) FROM casts WHERE role IS NOT NULL AND role != ''",
    "q006_trap_jobs": "SELECT COUNT(DISTINCT job_id) FROM casts",
    
    # Q007: "How many movies are in the database?" 
    # Correct: kind='movie' only (60579)
    # Trap: COUNT(*) FROM movies (211718) which includes episodes, seasons, series
    "q007_correct": "SELECT COUNT(*) FROM movies WHERE kind = 'movie'",
    "q007_trap": "SELECT COUNT(*) FROM movies",
    
    # Q008: "How many distinct categories are used to classify movies by genre?"
    # Correct: movie_categories JOIN categories WHERE root_id = 1 (genre subtree)
    # But to keep it ambiguous tier (not multi_table): how about just counting...
    # Actually: movie_categories has 613 distinct categories, movie_keywords has 13764
    # "classify movies" → movie_categories (genres, sources, etc.) vs movie_keywords (keywords)
    "q008_correct": "SELECT COUNT(DISTINCT category_id) FROM movie_categories",
    "q008_trap_keywords": "SELECT COUNT(DISTINCT category_id) FROM movie_keywords",
    
    # Q009: "What are the different types of relationships between movies?"
    # Correct: movie_references.type ('Remake', 'Influence', 'SpinOff', 'Parody', 'Homage')
    # Trap: movies.kind (episode, movie, season, series, movieseries)
    # Both are "types" of movie relationships
    "q009_correct": "SELECT DISTINCT type FROM movie_references ORDER BY type",
    "q009_trap_kind": "SELECT DISTINCT kind FROM movies ORDER BY kind",
    
    # Q010: "How many official title translations are recorded for movies?"
    # Correct: COUNT(*) WHERE official_translation = 1 (integer, not boolean!)
    # Trap: COUNT(*) FROM movie_aliases_iso (all, including unofficial)
    # The 'official_translation' column is an integer 0/1, not a boolean
    "q010_correct": "SELECT COUNT(*) FROM movie_aliases_iso WHERE official_translation = 1",
    "q010_trap": "SELECT COUNT(*) FROM movie_aliases_iso",
}

all_pass = True
for label, sql in sqls.items():
    r = execute_sql(DB, sql)
    if r is None:
        print(f"FAIL {label}: execution error")
        all_pass = False
    elif len(r[1]) == 0:
        print(f"FAIL {label}: 0 rows returned")
        all_pass = False
    else:
        print(f"OK   {label}: {r[1][0]}")

print(f"\n{'ALL PASS' if all_pass else 'FAILURES DETECTED'}")
