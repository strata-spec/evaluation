from scorer import execute_sql
DB='postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require'

# All 25 reference SQLs to verify
sqls = {
    "q001": "SELECT COUNT(*) FROM people",
    "q002": "SELECT DISTINCT kind FROM movies ORDER BY kind",
    "q003": "SELECT COUNT(*) FROM trailers",
    "q004": "SELECT COUNT(*) FROM movie_references",
    "q005": "SELECT COUNT(*) FROM movies WHERE runtime > 0",
    "q006": "SELECT COUNT(DISTINCT language) FROM movie_languages",
    "q007": "SELECT DISTINCT source FROM movie_links ORDER BY source",
    "q008": "SELECT COUNT(DISTINCT person_id) FROM people_aliases",
    "q009": "SELECT COUNT(DISTINCT language) FROM movie_aliases_iso",
    "q010": "SELECT COUNT(DISTINCT country) FROM movie_countries",
    "q011": "SELECT COUNT(DISTINCT mc.movie_id) FROM movie_categories mc JOIN categories c ON mc.category_id = c.id WHERE c.name = 'Drama'",
    "q012": "SELECT COUNT(DISTINCT c.person_id) FROM casts c JOIN jobs j ON c.job_id = j.id WHERE j.name = 'Director'",
    "q013": "SELECT COUNT(*) FROM movie_abstracts_en a JOIN movies m ON a.movie_id = m.id WHERE m.date < '2000-01-01'",
    "q014": "SELECT c.name, COUNT(*) as cnt FROM movie_keywords mk JOIN categories c ON mk.category_id = c.id GROUP BY c.name ORDER BY cnt DESC LIMIT 10",
    "q015": "SELECT COUNT(DISTINCT c.person_id) FROM casts c JOIN movies m ON c.movie_id = m.id WHERE m.date >= '2011-01-01'",
    "q016": "SELECT name, vote_average FROM movies WHERE vote_average IS NOT NULL AND vote_average > 0 AND kind = 'movie' ORDER BY vote_average DESC LIMIT 10",
    "q017": "SELECT name, votes_count FROM movies WHERE votes_count IS NOT NULL AND votes_count > 0 AND kind = 'movie' ORDER BY votes_count DESC LIMIT 10",
    "q018": "SELECT name, budget FROM movies WHERE budget IS NOT NULL AND budget > 0 AND kind = 'movie' ORDER BY budget DESC LIMIT 10",
    "q019": "SELECT name, revenue FROM movies WHERE revenue IS NOT NULL AND revenue > 0 AND kind = 'movie' ORDER BY revenue DESC LIMIT 10",
    "q020": "SELECT name, runtime FROM movies WHERE runtime IS NOT NULL AND runtime > 0 AND kind = 'movie' ORDER BY runtime DESC LIMIT 10",
    "q021": "SELECT COUNT(*) FROM people WHERE gender = 1",
    "q022": "SELECT COUNT(*) FROM movies WHERE homepage IS NOT NULL AND homepage != ''",
    "q023": "SELECT COUNT(*) FROM categories WHERE root_id = 1 AND id != 1",
    "q024": "SELECT COUNT(*) FROM casts c JOIN jobs j ON c.job_id = j.id WHERE j.name = 'Drehbuch'",
    "q025": "SELECT COUNT(*) FROM image_ids WHERE object_type = 'Movie'",
}

all_pass = True
for qid, sql in sqls.items():
    r = execute_sql(DB, sql)
    if r is None:
        print(f"FAIL {qid}: execution error")
        all_pass = False
    elif len(r[1]) == 0:
        print(f"FAIL {qid}: 0 rows returned")
        all_pass = False
    else:
        print(f"OK   {qid}: {len(r[1])} rows, first={r[1][0]}")

print(f"\n{'ALL PASS' if all_pass else 'FAILURES DETECTED'}")
