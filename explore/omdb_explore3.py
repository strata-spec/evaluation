from scorer import execute_sql
DB='postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require'

queries = [
    # Continue FK list
    ('more FKs', """SELECT tc.table_name, kcu.column_name, ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
        AND tc.table_name > 'movie_categories'
        ORDER BY tc.table_name"""),

    # category_names vs categories - difference
    ('category_names sample', "SELECT * FROM category_names LIMIT 10"),
    
    # job_names vs jobs
    ('job_names sample', "SELECT jn.job_id, jn.name, jn.language FROM job_names jn LIMIT 10"),
    
    # movie_keywords vs movie_categories - what is the difference?
    ('keyword roots', "SELECT DISTINCT c.root_id, cr.name FROM movie_keywords mk JOIN categories c ON mk.category_id = c.id JOIN categories cr ON c.root_id = cr.id LIMIT 10"),
    ('category roots for movie_categories', "SELECT DISTINCT c.root_id, cr.name FROM movie_categories mc JOIN categories c ON mc.category_id = c.id JOIN categories cr ON c.root_id = cr.id LIMIT 10"),
    
    # gender values - what do they mean?
    ('gender 0 people', "SELECT name, gender FROM people WHERE gender = 0 LIMIT 5"),
    ('gender 1 people', "SELECT name, gender FROM people WHERE gender = 1 LIMIT 5"),
    ('gender 2 people', "SELECT name, gender FROM people WHERE gender = 2 LIMIT 5"),
    
    # Runtime distribution  - check if it's minutes
    ('runtime samples', "SELECT name, runtime, kind FROM movies WHERE runtime IS NOT NULL AND runtime > 0 ORDER BY runtime DESC LIMIT 10"),
    ('short runtime', "SELECT name, runtime, kind FROM movies WHERE runtime IS NOT NULL AND runtime > 0 AND runtime < 10 LIMIT 5"),
    
    # Budget/revenue values
    ('budget samples', "SELECT name, budget, revenue FROM movies WHERE budget > 0 ORDER BY budget DESC LIMIT 10"),
    
    # movie_aliases_iso - what's official_translation?  
    ('aliases sample', "SELECT * FROM movie_aliases_iso LIMIT 5"),
    
    # image_ids object_type values
    ('image object types', "SELECT object_type, COUNT(*) FROM image_ids GROUP BY object_type ORDER BY COUNT(*) DESC"),
    
    # People with deathday (to check for deceased people queries)
    ('deceased count', "SELECT COUNT(*) FROM people WHERE deathday IS NOT NULL"),
    
    # Explore the date column in movies - is it release date?
    ('movies date samples', "SELECT id, name, date, kind FROM movies WHERE date IS NOT NULL AND kind = 'movie' ORDER BY date DESC LIMIT 10"),
    
    # vote_average - is it 1-10 scale?
    ('vote distribution', "SELECT ROUND(vote_average) as bucket, COUNT(*) FROM movies WHERE vote_average > 0 GROUP BY ROUND(vote_average) ORDER BY bucket"),
]
for label, q in queries:
    r = execute_sql(DB, q)
    if r: 
        print(f'\n=== {label} ===')
        print(f'cols: {r[0]}')
        for row in r[1][:15]: print(row)
