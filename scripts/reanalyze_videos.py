import asyncio
import sys
import os
import json

sys.path.append(os.getcwd())
from app.db.session import SessionLocal
from app.db.models import YoutubeVideo, YoutubeInsight
from app.config import get_settings
from app.ai.analyst import DeepSeekAnalyst
from app.ai.youtube_prompts import YOUTUBE_VIDEO_SYSTEM_PROMPT, build_video_analysis_prompt
from app.ai.analyst import _extract_json, _extract_message_text
from app.ai.vta_scorer import normalize_vta, validate_vta, compute_scores

async def reanalyze_videos():
    settings = get_settings()
    db = SessionLocal()
    
    # Check what videos have legacy insights
    legacy_video_ids = []
    insights = db.query(YoutubeInsight).all()
    for ins in insights:
        # Re-run for basically all insights to ensure high quality
        legacy_video_ids.append(ins.video_id)
        
    print(f"Found {len(legacy_video_ids)} Insights to re-analyze.")
    if not legacy_video_ids:
        print("Done.")
        return
        
    videos = db.query(YoutubeVideo).filter(YoutubeVideo.video_id.in_(legacy_video_ids)).all()
    
    analyst = DeepSeekAnalyst(settings)
    
    for v in videos:
        print(f"Re-analyzing via DeepSeek: {v.video_id} - {v.title}")
        if not v.transcript_text:
            print(f"  -> Skipping, no transcript available.")
            continue
            
        prompt = build_video_analysis_prompt(
            transcript=v.transcript_text,
            title=v.title,
            channel_title=v.channel_title or v.channel_id,
            published_at=v.published_at.isoformat() if v.published_at else "",
            symbol=settings.youtube_target_symbol,
        )
        
        try:
            api_kwargs = {
                "model": analyst.model,
                "messages": [
                    {"role": "system", "content": YOUTUBE_VIDEO_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                "max_tokens": 16384 if analyst._is_reasoner else 8192,
            }
            if not analyst._is_reasoner:
                api_kwargs["temperature"] = 0.3
                api_kwargs["response_format"] = {"type": "json_object"}

            response = await analyst._client.chat.completions.create(**api_kwargs)
            
            message = response.choices[0].message if response.choices else None
            content = _extract_message_text(message)
            json_str = _extract_json(content)
            
            final_vta = {}
            if json_str:
                try:
                    raw_data = json.loads(json_str)
                    vta = normalize_vta(raw_data)
                    is_valid, errors = validate_vta(vta)
                    
                    if not is_valid:
                        vta['provenance']['schema_errors'] = errors
                        vta['provenance']['scores'] = None
                    else:
                        try:
                            scores = compute_scores(vta)
                            vta['provenance']['scores'] = scores
                        except Exception as e:
                            vta['provenance']['scores'] = None
                            vta['provenance']['schema_errors'].append(f"Scoring error res: {e}")
                    
                    final_vta = vta
                    
                    # Update DB Insight
                    ins = db.query(YoutubeInsight).filter_by(video_id=v.video_id).order_by(YoutubeInsight.created_at.desc()).first()
                    if ins:
                        ins.analyst_view_json = final_vta
                        db.commit()
                        print(f"  -> Successfully re-scored and updated DB. VSI={final_vta.get('provenance', {}).get('scores', {}).get('vsi', 'N/A')}")
                    else:
                        print("  -> Could not locate insight in DB to update")
                except json.JSONDecodeError as je:
                    print(f"  -> DeepSeek JSON Decode Error (possibly truncated?): {je}")
            else:
                print("  -> No JSON found in response, skipping DB update.")
                
        except Exception as e:
            print(f"  -> DeepSeek API Error: {e}")

if __name__ == "__main__":
    asyncio.run(reanalyze_videos())
    print("Re-analysis complete.")
