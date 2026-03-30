from scorer import execute_sql
DB='postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require'

queries = [
    # Explore categories tree
    ('top categories', "SELECT c.name, COUNT(*) FROM categories c WHERE c.parent_id IS NULL GROUP BY c.name ORDER BY COUNT(*) DESC LIMIT 10"),
    ('category roots', "SELECT c.name, c.id FROM categories c WHERE c.root_id = c.id ORDER BY c.name LIMIT 15"),
    # Jobs
    ('job types', "SELECT name, COUNT(*) FROM jobs GROUP BY name ORDER BY COUNT(*) DESC LIMIT 15"),
    # People gender values
    ('gender values', "SELECT gender, COUNT(*) FROM people GROUP BY gender ORDER BY COUNT(*) DESC"),
    # movie_references types
    ('ref types', "SELECT type, COUNT(*) FROM movie_references GROUP BY type ORDER BY COUNT(*) DESC LIMIT 10"),
    # movie_links sources
    ('link sources', "SELECT source, COUNT(*) FROM movie_links GROUP BY source ORDER BY COUNT(*) DESC"),
    # trailers sources
    ('trailer sources', "SELECT source, COUNT(*) FROM trailers GROUP BY source ORDER BY COUNT(*) DESC"),
    # casts sample
    ('casts sample', "SELECT * FROM casts LIMIT 5"),
    # movies with episodes (parent_id)
    ('episode example', "SELECT id, name, parent_id, kind, date FROM movies WHERE kind = 'episode' LIMIT 5"),
    # series example
    ('series example', "SELECT id, name, kind, date FROM movies WHERE kind = 'series' LIMIT 5"),
    # season example
    ('season example', "SELECT id, name, parent_id, kind, date FROM movies WHERE kind = 'season' LIMIT 5"),
    # votes_count seems low - check max
    ('top votes_count', "SELECT id, name, votes_count, vote_average FROM movies ORDER BY votes_count DESC NULLS LAST LIMIT 10"),
    # columns appearing in multiple tables (ambiguity source)
    ('name col tables', """SELECT table_name FROM information_schema.columns
        WHERE column_name = 'name' AND table_schema = 'public' ORDER BY table_name"""),
    ('movie_id col tables', """SELECT table_name FROM information_schema.columns
        WHERE column_name = 'movie_id' AND table_schema = 'public' ORDER BY table_name"""),
    # casts_view - is it a view?
    ('casts_view type', """SELECT table_type FROM information_schema.tables
        WHERE table_name = 'casts_view' AND table_schema = 'public'"""),
    # movie_categories sample
    ('movie_categories sample', "SELECT mc.movie_id, c.name FROM movie_categories mc JOIN categories c ON mc.category_id = c.id LIMIT 10"),
    # languages sample
    ('languages sample', "SELECT language, COUNT(*) FROM movie_languages GROUP BY language ORDER BY COUNT(*) DESC LIMIT 10"),
    # countries sample
    ('countries sample', "SELECT country, COUNT(*) FROM movie_countries GROUP BY country ORDER BY COUNT(*) DESC LIMIT 10"),
    # movie_keywords - what are they?
    ('keywords sample', "SELECT mk.movie_id, c.name FROM movie_keywords mk JOIN categories c ON mk.category_id = c.id LIMIT 10"),
    # FK constraints
    ('foreign_keys', """SELECT tc.table_name, kcu.column_name, ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
        ORDER BY tc.table_name"""),
]
for label, q in queries:
    r = execute_sql(DB, q)
    if r: 
        print(f'\n=== {label} ===')
        print(f'cols: {r[0]}')
        for row in r[1][:15]: print(row)
