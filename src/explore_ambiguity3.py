from scorer import execute_sql
DB='postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require'

queries = [
    # q008 candidate: cast entries with named character
    ('role nonempty count', "SELECT COUNT(*) FROM casts WHERE role != ''"),
    ('role empty count', "SELECT COUNT(*) FROM casts WHERE role = ''"),
    
    # q009 candidate: types of references
    ('ref types', "SELECT DISTINCT type FROM movie_references ORDER BY type"),
    
    # q010 candidate: jobs vs job_names for English job titles
    ('jobs count', "SELECT COUNT(*) FROM jobs"),
    ('job_names en count', "SELECT COUNT(*) FROM job_names WHERE language = 'en'"),
    # Are they the same set?
    ('jobs not in job_names_en', """SELECT COUNT(*) FROM jobs j 
        WHERE NOT EXISTS (SELECT 1 FROM job_names jn WHERE jn.job_id = j.id AND jn.language = 'en')"""),
    ('sample mismatch', """SELECT j.id, j.name FROM jobs j 
        WHERE NOT EXISTS (SELECT 1 FROM job_names jn WHERE jn.job_id = j.id AND jn.language = 'en') LIMIT 10"""),
    
    # Drama sub-genres
    ('drama id', "SELECT id, name FROM categories WHERE name = 'Drama' AND root_id = 1"),
    ('drama sub-genres', "SELECT COUNT(*) FROM categories WHERE parent_id = 18"),
    ('drama sub-genre list', "SELECT name FROM categories WHERE parent_id = 18 ORDER BY name"),
    
    # Additional checks for q006/q007
    ('casts role distinct nonempty', "SELECT COUNT(DISTINCT role) FROM casts WHERE role != ''"),
    ('movies kind movie', "SELECT COUNT(*) FROM movies WHERE kind = 'movie'"),
    
    # Can I find more where table name doesn't help?
    # image_ids.object_type = 'Person' vs people table
    # "How many people have an image?"
    ('people with images', "SELECT COUNT(*) FROM image_ids WHERE object_type = 'Person'"),
    ('people count', "SELECT COUNT(*) FROM people"),
    
    # casts entries with role for job_id = 15 (Actor) specifically
    ('actor entries with role', "SELECT COUNT(*) FROM casts WHERE job_id = 15 AND role != ''"),
    ('actor entries total', "SELECT COUNT(*) FROM casts WHERE job_id = 15"),
    ('non-actor entries with role', "SELECT COUNT(*) FROM casts WHERE job_id != 15 AND role != ''"),
]

for label, q in queries:
    r = execute_sql(DB, q)
    if r:
        print(f'\n=== {label} ===')
        for row in r[1][:20]: print(row)
