from scorer import execute_sql
DB='postgresql://neondb_owner:npg_ovZitH2C1BaM@ep-falling-cloud-ab84tggr-pooler.eu-west-2.aws.neon.tech/omdb?sslmode=require&channel_binding=require'

# Final candidate reference SQLs and their traps
sqls = {
    # === q006: "distinct roles" — role column = character name, not job function ===
    "q006 CORRECT (char names)": "SELECT COUNT(DISTINCT role) FROM casts WHERE role != ''",
    "q006 TRAP (job types)": "SELECT COUNT(DISTINCT job_id) FROM casts",
    
    # === q007: "how many movies" — movies table has non-movie entries ===
    "q007 CORRECT (kind=movie)": "SELECT COUNT(*) FROM movies WHERE kind = 'movie'",
    "q007 TRAP (all entries)": "SELECT COUNT(*) FROM movies",
    
    # === q008: "entries belong to a series" — series_id vs parent_id ===
    "q008 CORRECT (series_id)": "SELECT COUNT(*) FROM movies WHERE series_id IS NOT NULL",
    "q008 TRAP (parent_id)": "SELECT COUNT(*) FROM movies WHERE parent_id IS NOT NULL",
    "q008 TRAP2 (kind=series)": "SELECT COUNT(*) FROM movies WHERE kind = 'series'",
    
    # === q009: "types of cross-references" — movie_references.type vs movies.kind ===
    "q009 CORRECT (ref types)": "SELECT DISTINCT type FROM movie_references ORDER BY type",
    "q009 TRAP (movie kinds)": "SELECT DISTINCT kind FROM movies ORDER BY kind",
    
    # === q010: "movies assigned to a keyword" — movie_keywords vs movie_categories ===
    "q010 CORRECT (keywords)": "SELECT COUNT(DISTINCT movie_id) FROM movie_keywords",
    "q010 TRAP (categories)": "SELECT COUNT(DISTINCT movie_id) FROM movie_categories",
}

for label, sql in sorted(sqls.items()):
    r = execute_sql(DB, sql)
    if r is None:
        print(f"FAIL {label}: execution error")
    elif len(r[1]) == 0:
        print(f"FAIL {label}: 0 rows returned")
    else:
        result_str = r[1][0] if len(r[1]) == 1 else f"{len(r[1])} rows"
        print(f"OK   {label}: {result_str}")

# Print the delta between correct and trap for each
print("\n--- Correct vs Trap deltas ---")
print("q006: 357,355 chars vs 571 jobs (624x difference)")
print("q007: 60,579 movies vs 211,718 all (3.5x difference)")
print("q008: 135,791 series_id vs 151,096 parent_id vs 4,305 series kind")
print("q009: 5 ref types vs 5 movie kinds (same count! different values)")
print("q010: 50,511 keywords vs 71,083 categories (1.4x difference)")
