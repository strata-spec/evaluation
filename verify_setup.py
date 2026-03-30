#!/usr/bin/env python3
"""
Verification script for Strata eval setup.
Tests that all components work together without requiring LLM calls or database access.
This is a non-LLM verification run.
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from scorer import load_questions, compute_metrics, compute_metrics_by_tier
    from reporter import generate_report, write_report
    from smif_loader import load_semantic, load_merged
    print("✓ All imports successful")
except ImportError as e:
    print(f"✗ Import failed: {e}")
    sys.exit(1)

def verify_file_structure() -> bool:
    """Verify that all required directories exist."""
    print("\n--- Verifying File Structure ---")
    required_dirs = [
        "src",
        "tests", 
        "config",
        "questions",
        "explore",
        "strata",
        "strata/config",
        "strata/output",
        "results",
    ]
    
    all_exist = True
    for dir_path in required_dirs:
        full_path = Path(".") / dir_path
        exists = full_path.exists() and full_path.is_dir()
        status = "✓" if exists else "✗"
        print(f"{status} {dir_path}/")
        all_exist = all_exist and exists
    
    return all_exist

def verify_questions() -> bool:
    """Load and verify questions structure."""
    print("\n--- Verifying Questions ---")
    try:
        questions_path = Path("questions/omdb.yaml")
        if not questions_path.exists():
            print(f"✗ Questions file not found: {questions_path}")
            return False
        
        questions = load_questions(str(questions_path))
        print(f"✓ Loaded {len(questions)} questions")
        
        # Verify by tier
        tiers = {}
        for q in questions:
            tier = q.get("tier")
            tiers[tier] = tiers.get(tier, 0) + 1
        
        print(f"✓ Questions by tier:")
        for tier in sorted(tiers.keys()):
            print(f"  - {tier}: {tiers[tier]} question(s)")
        
        return True
    except Exception as e:
        print(f"✗ Error loading questions: {e}")
        return False

def verify_semantic() -> bool:
    """Load and verify semantic model structure."""
    print("\n--- Verifying Semantic Model ---")
    try:
        semantic_path = Path("config/semantic.yaml")
        if not semantic_path.exists():
            print(f"⚠ Semantic file not found: {semantic_path} (optional for non-LLM run)")
            return True
        
        semantic = load_semantic(str(semantic_path))
        print(f"✓ Loaded semantic model")
        
        domain = semantic.get("domain", {})
        print(f"✓ Domain: {domain.get('name', 'unknown')}")
        
        models = semantic.get("models", [])
        print(f"✓ Models: {len(models)} model(s)")
        
        relationships = semantic.get("relationships", [])
        print(f"✓ Relationships: {len(relationships)} relationship(s)")
        
        return True
    except Exception as e:
        print(f"✗ Error loading semantic: {e}")
        return False

def verify_corrections() -> bool:
    """Load and verify corrections structure."""
    print("\n--- Verifying Corrections ---")
    try:
        corrections_path = Path("config/corrections.yaml")
        if not corrections_path.exists():
            print(f"⚠ Corrections file not found: {corrections_path} (optional)")
            return True
        
        corrections = load_semantic(str(corrections_path))  # Same YAML structure
        print(f"✓ Loaded corrections")
        
        # Verify merged model with corrections
        semantic_path = Path("config/semantic.yaml")
        if semantic_path.exists():
            merged = load_merged(str(semantic_path), str(corrections_path))
            print(f"✓ Merged semantic + corrections successfully")
        
        return True
    except Exception as e:
        print(f"✗ Error loading corrections: {e}")
        return False

def verify_reporter() -> bool:
    """Verify reporter can generate report from mock results."""
    print("\n--- Verifying Reporter ---")
    try:
        # Create mock results
        mock_results = [
            {
                "id": "q001",
                "tier": "simple",
                "condition": "baseline",
                "natural_language": "How many test records?",
                "generated_sql": "SELECT COUNT(*) FROM test",
                "reference_sql": "SELECT COUNT(*) FROM test",
                "execution_success": True,
                "result_match": True,
                "schema_link_correct": True,
                "failure_mode": None,
                "notes": "Mock test case",
            },
            {
                "id": "q001",
                "tier": "simple",
                "condition": "strata",
                "natural_language": "How many test records?",
                "generated_sql": "SELECT COUNT(*) FROM test",
                "reference_sql": "SELECT COUNT(*) FROM test",
                "execution_success": True,
                "result_match": True,
                "schema_link_correct": True,
                "failure_mode": None,
                "notes": "Mock test case",
            },
        ]
        
        # Test metrics computation
        metrics = compute_metrics(mock_results)
        print(f"✓ Computed metrics: {metrics}")
        
        # Test metrics by tier
        metrics_by_tier = compute_metrics_by_tier(mock_results)
        print(f"✓ Computed metrics by tier: {len(metrics_by_tier)} tier(s)")
        
        # Test report generation
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        report = generate_report(mock_results, run_id)
        print(f"✓ Generated report ({len(report)} chars)")
        
        # Test report writing
        results_dir = Path("results")
        results_dir.mkdir(parents=True, exist_ok=True)
        report_path = write_report(mock_results, run_id, str(results_dir))
        print(f"✓ Wrote report to {report_path}")
        
        # Verify file exists
        if Path(report_path).exists():
            print(f"✓ Report file exists and is readable")
        else:
            print(f"✗ Report file was not created")
            return False
        
        return True
    except Exception as e:
        print(f"✗ Error in reporter: {e}")
        import traceback
        traceback.print_exc()
        return False

def main() -> int:
    """Run all verification tests."""
    print("=" * 60)
    print("STRATA EVAL - NON-LLM VERIFICATION RUN")
    print("=" * 60)
    print()
    print(f"Project root: {Path().absolute()}")
    print()
    
    results = {
        "structure": verify_file_structure(),
        "questions": verify_questions(),
        "semantic": verify_semantic(),
        "corrections": verify_corrections(),
        "reporter": verify_reporter(),
    }
    
    # Summary
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    for check, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {check}")
    
    all_passed = all(results.values())
    print()
    if all_passed:
        print("✓ All verifications passed!")
        print()
        print("Next steps:")
        print("1. Add strata configuration to strata/config/")
        print("2. Update config/semantic.yaml with strata output")
        print("3. Update config/corrections.yaml as needed")
        print("4. Run evaluation with LLM:")
        print("   python -m src.eval \\")
        print("     --db <postgres_connection_string> \\")
        print("     --model <anthropic_model> \\")
        print("     --semantic config/semantic.yaml \\")
        print("     --corrections config/corrections.yaml")
        return 0
    else:
        print("✗ Some verifications failed. Please review the output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
