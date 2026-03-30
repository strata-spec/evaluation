from scorer import execute_sql
DB='postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require'

queries = [
    # Candidate q008: plot summary availability
    ('abstracts_en distinct', "SELECT COUNT(DISTINCT movie_id) FROM movie_abstracts_en"),
    ('abstracts_de distinct', "SELECT COUNT(DISTINCT movie_id) FROM movie_abstracts_de"),
    ('abstracts_es distinct', "SELECT COUNT(DISTINCT movie_id) FROM movie_abstracts_es"),
    ('abstracts_fr distinct', "SELECT COUNT(DISTINCT movie_id) FROM movie_abstracts_fr"),
    ('all abstracts union', """SELECT COUNT(DISTINCT movie_id) FROM (
        SELECT movie_id FROM movie_abstracts_en
        UNION SELECT movie_id FROM movie_abstracts_de
        UNION SELECT movie_id FROM movie_abstracts_es
        UNION SELECT movie_id FROM movie_abstracts_fr
    ) sub"""),
    
    # Candidate q010: movies tagged with terms
    ('movie_keywords distinct movies', "SELECT COUNT(DISTINCT movie_id) FROM movie_keywords"),
    ('movie_categories distinct movies', "SELECT COUNT(DISTINCT movie_id) FROM movie_categories"),
    
    # Explore: position in casts — what does it mean?
    ('casts position range', "SELECT MIN(position), MAX(position), AVG(position) FROM casts WHERE position IS NOT NULL"),
    ('casts position sample', "SELECT c.role, c.position FROM casts c WHERE c.role != '' AND c.position IS NOT NULL ORDER BY c.movie_id, c.position LIMIT 10"),
    
    # Check: casts entries where role is empty vs non-empty
    ('role empty', "SELECT COUNT(*) FROM casts WHERE role = ''"),
    ('role non-empty', "SELECT COUNT(*) FROM casts WHERE role != '' AND role IS NOT NULL"),
    ('role null', "SELECT COUNT(*) FROM casts WHERE role IS NULL"),
    
    # movies with homepages — how many are actually non-empty strings?
    ('homepage null', "SELECT COUNT(*) FROM movies WHERE homepage IS NULL"),
    ('homepage empty string', "SELECT COUNT(*) FROM movies WHERE homepage = ''"),
    ('homepage non-empty', "SELECT COUNT(*) FROM movies WHERE homepage IS NOT NULL AND homepage != ''"),
    
    # Verify: movies with budget (0 vs NULL vs positive)
    ('budget null', "SELECT COUNT(*) FROM movies WHERE budget IS NULL"),
    ('budget zero', "SELECT COUNT(*) FROM movies WHERE budget = 0"),    
    ('budget positive', "SELECT COUNT(*) FROM movies WHERE budget > 0"),

    # How many people have deathday NOT NULL? (for "living" vs "deceased" ambiguity concept)
    ('deathday not null', "SELECT COUNT(*) FROM people WHERE deathday IS NOT NULL"),
    ('both dates', "SELECT COUNT(*) FROM people WHERE birthday IS NOT NULL AND deathday IS NOT NULL"),
    
    # movies where date IS NULL
    ('date null movies', "SELECT COUNT(*) FROM movies WHERE date IS NULL AND kind = 'movie'"),
    ('date nonnull movies', "SELECT COUNT(*) FROM movies WHERE date IS NOT NULL AND kind = 'movie'"),
    
    # casts_view — can baseline agent be tricked into using it?
    ('casts_view columns', """SELECT column_name, data_type FROM information_schema.columns 
        WHERE table_name = 'casts_view' ORDER BY ordinal_position"""),
]

for label, q in queries:
    r = execute_sql(DB, q)
    if r:
        print(f'\n=== {label} ===')
        for row in r[1][:10]: print(row)
