from scorer import execute_sql
DB='postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require'

queries = [
    # What is Drehbuch? 
    ('drehbuch in job_names', "SELECT jn.name, jn.language FROM job_names jn WHERE jn.job_id = 100"),
    
    # Check genre subcategories structure
    ('genre subcats', "SELECT c.id, c.name, c.parent_id FROM categories c WHERE c.root_id = 1 ORDER BY c.name LIMIT 30"),
    
    # Source categories
    ('source cats', "SELECT c.id, c.name FROM categories c WHERE c.root_id = 2 ORDER BY c.name LIMIT 20"),
    
    # Standing categories (what is this?)
    ('standing cats', "SELECT c.id, c.name FROM categories c WHERE c.root_id = 4 ORDER BY c.name"),
    
    # Term categories
    ('term cats', "SELECT c.id, c.name FROM categories c WHERE c.root_id = 9 ORDER BY c.name LIMIT 20"),
    
    # How many movies (kind='movie') have vote data?
    ('movie votes', "SELECT COUNT(*) FROM movies WHERE kind = 'movie' AND votes_count > 0"),
    
    # Check: are there movies with runtime = 0 vs NULL?
    ('runtime zero', "SELECT COUNT(*) FROM movies WHERE runtime = 0"),
    ('runtime null', "SELECT COUNT(*) FROM movies WHERE runtime IS NULL"),
    ('runtime positive', "SELECT COUNT(*) FROM movies WHERE runtime > 0"),
    
    # Verify some candidate reference SQLs
    # Simple q1: count of people
    ('q_people_count', "SELECT COUNT(*) FROM people"),
    # Simple q2: count of trailers
    ('q_trailers', "SELECT COUNT(*) FROM trailers"),
    # Simple q3: distinct kinds
    ('q_kinds', "SELECT DISTINCT kind FROM movies ORDER BY kind"),
    # Simple q4: movies with homepage
    ('q_homepage', "SELECT COUNT(*) FROM movies WHERE homepage IS NOT NULL"),
    # Simple q5: movie references count
    ('q_refs', "SELECT COUNT(*) FROM movie_references"),
    
    # Verify movie count for kind = 'movie' specifically
    ('movies only', "SELECT COUNT(*) FROM movies WHERE kind = 'movie'"),
    
    # Check: how many movies have vote_average >= 8?
    ('high rated', "SELECT COUNT(*) FROM movies WHERE vote_average >= 8"),

    # movies by decade
    ('movies_2000s', "SELECT COUNT(*) FROM movies WHERE date >= '2000-01-01' AND date < '2010-01-01' AND kind = 'movie'"),
    
    # Check Actors department ID
    ('actor dept', "SELECT id, name FROM jobs WHERE id = 4"),
    ('actor role', "SELECT id, name FROM jobs WHERE id = 15"),
    
    # Movies with multiple countries
    ('multi country', "SELECT movie_id, COUNT(*) as cnt FROM movie_countries GROUP BY movie_id HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 5"),
    
    # Verify: movies with abstracts
    ('abstracts_en', "SELECT COUNT(*) FROM movie_abstracts_en"),
]
for label, q in queries:
    r = execute_sql(DB, q)
    if r: 
        print(f'\n=== {label} ===')
        print(f'cols: {r[0]}')
        for row in r[1][:20]: print(row)
