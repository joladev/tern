use types::Enrichment;

const SECONDS_PER_DAY: i64 = 86_400;
const FOLLOWER_WEIGHT: f64 = 100.0;
const AGE_WEIGHT: f64 = 10.0;

pub trait Scorer: Send {
    fn score(&self, enrichment: &Enrichment, now: i64) -> u64;
}

pub struct Reach;

impl Scorer for Reach {
    fn score(&self, enrichment: &Enrichment, now: i64) -> u64 {
        let followers = (1.0 + enrichment.followers as f64).log2();
        let age_days = enrichment
            .created_at
            .map_or(0, |created_at| (now - created_at) / SECONDS_PER_DAY);
        let age = (1.0 + age_days.max(0) as f64).log2();
        (followers * FOLLOWER_WEIGHT + age * AGE_WEIGHT) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_800_000_000;

    fn score(followers: u64, age_days: i64) -> u64 {
        let enrichment = Enrichment {
            followers,
            created_at: Some(NOW - age_days * SECONDS_PER_DAY),
        };
        Reach.score(&enrichment, NOW)
    }

    #[test]
    fn more_followers_score_higher() {
        assert!(score(1_000, 30) > score(10, 30));
    }

    #[test]
    fn age_breaks_ties_between_similar_accounts() {
        assert!(score(100, 500) > score(100, 5));
    }

    #[test]
    fn followers_outweigh_age() {
        assert!(score(1_000, 1) > score(10, 2_000));
    }

    #[test]
    fn a_future_creation_date_does_not_underflow() {
        assert_eq!(score(0, -10), 0);
    }
}
