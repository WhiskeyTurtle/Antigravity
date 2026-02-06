
import random

class Analyzer:
    def __init__(self):
        self.min_velocity = 50  # Minimum tx per minute to consider "active"

    def score_token(self, token_data):
        """
        Scoring Logic (Growth & Popularity):
        - Volume Score (0-50): Based on 5m volume (Target $10k+)
        - Trend Score (0-30): Based on 5m price change (Target +10%+)
        - Liquidity Score (0-20): Safety check (Target $5k+)
        """
        score = 0
        verdict = "WATCH"
        
        # 1. Volume Score (Max 50)
        vol_m5 = token_data.get("volume_m5", 0)
        if vol_m5 > 10000:
            score += 50
        elif vol_m5 > 5000:
            score += 35
        elif vol_m5 > 1000:
            score += 15
        elif vol_m5 > 100:
            score += 5
            
        # 2. Trend Score (Max 30)
        change_m5 = token_data.get("price_change_m5", 0)
        if change_m5 > 20:
            score += 30
        elif change_m5 > 10:
            score += 20
        elif change_m5 > 0:
            score += 10
            
        # 3. Liquidity Safety (Max 20)
        liq = token_data.get("liquidity_usd", 0)
        if liq > 10000:
            score += 20
        elif liq > 2000:
            score += 10
        elif liq < 500:
            score -= 50 # Penalty for low liquidity (Rug risk)
            
        # Verdict Thresholds
        if score >= 60 and liq > 1000:
            verdict = "BUY"
            
        # Add Analysis Note
        analysis_note = []
        if vol_m5 > 5000: analysis_note.append("🔥 High Vol")
        if change_m5 > 10: analysis_note.append("🚀 Pumping")
        if liq < 1000: analysis_note.append("⚠️ Low Liq")
        
        return {
            "token": token_data.get("token", "UNKNOWN"),
            "score": score,
            "verdict": verdict,
            "metrics": {
                "vol_m5": vol_m5,
                "change_m5": change_m5,
                "liq": liq
            },
            "risk_msg": " ".join(analysis_note) if analysis_note else "Waiting for volume..."
        }

    def score_social(self, social_data):
        """
        Scoring Logic (Social Conviction):
        - Impact Score: Influencer weight (0-100)
        - Multiplier: High Impact = Buy Signal regardless of tech
        """
        impact = social_data.get("impact_score", 0)
        author = social_data.get("author", "Unknown")
        
        score = impact # Direct mapping for now
        verdict = "WATCH"
        
        if impact >= 90:
            score += 200 # Massive boost
            verdict = "SUPER_ALPHA"
        elif impact >= 50:
            score += 50
            verdict = "BUY"
            
        return {
            "token": social_data.get("token"),
            "score": score,
            "verdict": verdict,
            "risk_msg": f"🐦 Social Alpha: {author} (Impact: {impact})"
        }
