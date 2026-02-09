
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
        
        # 1. Volume Check (Max 30)
        vol_m5 = token_data.get("volume_m5", token_data.get("volume_h1", 0) / 12)
        if vol_m5 > 50000:
            score += 30
        elif vol_m5 > 10000:
            score += 20
        elif vol_m5 > 1000:
            score += 15 # Boosted from 10
            
        # 2. Price Momentum (Max 30)
        price_change_m5 = token_data.get("price_change_m5", 0)
        if price_change_m5 > 50:
            score += 30
        elif price_change_m5 > 20:
            score += 20
        elif price_change_m5 > 0:
            score += 10
            
        # 3. Liquidity Safety (Max 30)
        liq = token_data.get("liquidity_usd", 0)
        if liq > 25000:
            score += 30
        elif liq > 10000:
            score += 25 # Boosted from 15
        elif liq > 5000:
            score += 10 # New Tier
        elif liq < 4000:
            score -= 100 # Relaxed slightly from 5k
            
        # Verdict Thresholds
        # Require higher score and safe liquidity
        if score >= 50 and liq > 8000:
            verdict = "BUY"
            
        print(f"📊 ANALYZER: {token_data.get('token', '???')[:8]} | Score: {score} | Liq: ${liq:,.0f} | Verdict: {verdict}")
        
        # Add Analysis Note
        analysis_note = []
        if vol_m5 > 5000: analysis_note.append("🔥 High Vol")
        if price_change_m5 > 10: analysis_note.append("🚀 Pumping")
        if liq < 1000: analysis_note.append("⚠️ Low Liq")
        
        return {
            "token": token_data.get("token", "UNKNOWN"),
            "score": score,
            "verdict": verdict,
            "metrics": {
                "vol_m5": vol_m5,
                "change_m5": price_change_m5,
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
