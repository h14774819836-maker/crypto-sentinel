import sys
import os

sys.path.append(os.getcwd())
from app.db.session import SessionLocal
from app.db.models import YoutubeInsight
from app.ai.vta_scorer import adapt_legacy, compute_scores

def migrate_legacy_insights():
    db = SessionLocal()
    insights = db.query(YoutubeInsight).all()
    updated = 0
    for ins in insights:
        if ins.analyst_view_json and 'vta_version' not in ins.analyst_view_json:
            # This is a legacy JSON
            print(f"Migrating Insight ID: {ins.id} for Video {ins.video_id}")
            vta = adapt_legacy(ins.analyst_view_json)
            try:
                scores = compute_scores(vta)
                vta['provenance']['scores'] = scores
            except Exception as e:
                vta['provenance']['scores'] = None
                vta.setdefault('provenance', {}).setdefault('schema_errors', []).append(f"Scoring error: {e}")
            
            ins.analyst_view_json = vta
            updated += 1

    if updated > 0:
        db.commit()
        print(f"Updated {updated} legacy insights to VTA-JSON v1.")
    else:
        print("No legacy insights found to update.")

if __name__ == "__main__":
    migrate_legacy_insights()
