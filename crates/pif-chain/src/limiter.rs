//! Per-endpoint rate limiting: a token bucket that converges on a limit nobody will tell it.
//!
//! **Per endpoint, never global.** A shared limiter would let the slowest provider throttle
//! the fastest, which is the opposite of what several endpoints are for.
//!
//! The permitted rate moves by additive-increase / multiplicative-decrease, because that is
//! the behaviour that converges on an unknown ceiling without being told what it is — and a
//! public endpoint will never say. Every success creeps the rate up by a fixed step; every
//! 429 or timeout halves it. Persistent failure trips a circuit breaker, so a dead provider
//! costs one probe per backoff rather than a request per attempt.
//!
//! Time is a parameter rather than a call to `Instant::now()` inside the state machine, so
//! every behaviour below is a deterministic test rather than a sleep and a hope.

use std::time::{Duration, Instant};

/// Where the rate starts, and the floor a recovering endpoint returns to.
///
/// Deliberately timid. Being wrong upwards means a ban; being wrong downwards costs a few
/// seconds of creeping back up.
const FLOOR_RPS: f64 = 2.0;

/// Ceiling when the config does not name one.
const DEFAULT_MAX_RPS: f64 = 20.0;

/// Added to the rate on each success.
const ADDITIVE_STEP: f64 = 0.5;

/// Consecutive failures that open the breaker.
const FAILURES_TO_OPEN: u32 = 5;

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// What a caller should do right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Go ahead; a token has been spent.
    Ready,
    /// The bucket is empty. Wait this long and ask again.
    Wait(Duration),
    /// The breaker is open. This endpoint is out until the backoff expires.
    Open(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Breaker {
    Closed,
    /// Nothing is sent until `until`; then one probe is allowed through.
    Open {
        until: Instant,
    },
    /// A probe is in flight. Its outcome decides whether the breaker closes or reopens.
    HalfOpen,
}

/// One endpoint's share of the request budget.
pub struct Limiter {
    rate: f64,
    max_rate: f64,
    tokens: f64,
    last_refill: Instant,
    consecutive_failures: u32,
    breaker: Breaker,
    backoff: Duration,
    /// Cheap deterministic jitter, so several endpoints tripping together do not all probe
    /// on the same tick. A full RNG dependency would buy nothing here — this only has to be
    /// uncorrelated between endpoints, which seeding from the URL achieves.
    jitter: u64,
}

impl Limiter {
    pub fn new(max_rps: Option<u32>, url: &str, now: Instant) -> Self {
        let max_rate = max_rps.map(f64::from).unwrap_or(DEFAULT_MAX_RPS);
        Self {
            rate: FLOOR_RPS.min(max_rate),
            max_rate,
            tokens: 1.0,
            last_refill: now,
            consecutive_failures: 0,
            breaker: Breaker::Closed,
            backoff: INITIAL_BACKOFF,
            jitter: seed_from(url),
        }
    }

    /// The rate currently permitted, in calls per second. For logging and tests.
    pub fn rate(&self) -> f64 {
        self.rate
    }

    pub fn is_open(&self) -> bool {
        matches!(self.breaker, Breaker::Open { .. })
    }

    /// Try to spend a token.
    pub fn poll(&mut self, now: Instant) -> Decision {
        if let Breaker::Open { until } = self.breaker {
            if now < until {
                return Decision::Open(until - now);
            }
            // The backoff has expired: let exactly one request through to find out whether
            // the endpoint is back, rather than resuming at full rate and being banned again.
            self.breaker = Breaker::HalfOpen;
            return Decision::Ready;
        }

        self.refill(now);

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Decision::Ready;
        }

        // Time until the next whole token, so the caller sleeps exactly long enough.
        let missing = 1.0 - self.tokens;
        Decision::Wait(Duration::from_secs_f64(missing / self.rate))
    }

    /// Record a successful call.
    pub fn succeeded(&mut self) {
        self.consecutive_failures = 0;

        match self.breaker {
            Breaker::HalfOpen => {
                // Back from the dead. Resume at the floor, not at the rate that got us
                // banned, and reset the backoff so the next outage starts short again.
                self.breaker = Breaker::Closed;
                self.rate = FLOOR_RPS.min(self.max_rate);
                self.backoff = INITIAL_BACKOFF;
            }
            _ => {
                self.breaker = Breaker::Closed;
                self.rate = (self.rate + ADDITIVE_STEP).min(self.max_rate);
            }
        }
    }

    /// Record a rejected or timed-out call.
    ///
    /// Halving rather than stepping down is the whole point of AIMD: backing off slowly from
    /// a limit you have already crossed keeps crossing it.
    pub fn failed(&mut self, now: Instant) {
        self.rate = (self.rate / 2.0).max(FLOOR_RPS.min(self.max_rate));

        if self.breaker == Breaker::HalfOpen {
            // The probe failed, so the endpoint is still down. Double the wait rather than
            // probing again immediately.
            self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
            self.open(now);
            return;
        }

        self.consecutive_failures += 1;
        if self.consecutive_failures >= FAILURES_TO_OPEN {
            self.open(now);
        }
    }

    fn open(&mut self, now: Instant) {
        // Jittered so that endpoints which failed together do not recover in lockstep and
        // hammer whatever they share.
        let wait = self.backoff + Duration::from_millis(self.next_jitter_ms());
        self.breaker = Breaker::Open { until: now + wait };
        self.consecutive_failures = 0;
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        self.last_refill = now;
        // Capped at one second's worth: a limiter idle for an hour must not be allowed to
        // spend an hour of requests at once, which is exactly how a burst gets you banned.
        self.tokens = (self.tokens + elapsed * self.rate).min(self.rate.max(1.0));
    }

    /// xorshift64*, inlined. Deterministic per limiter, uncorrelated between them.
    fn next_jitter_ms(&mut self) -> u64 {
        self.jitter ^= self.jitter << 13;
        self.jitter ^= self.jitter >> 7;
        self.jitter ^= self.jitter << 17;
        self.jitter % 1000
    }
}

/// A non-zero seed derived from the endpoint URL.
fn seed_from(url: &str) -> u64 {
    // FNV-1a. Not a hash anyone should rely on for anything, which is why it is here rather
    // than pulled in as a dependency.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in url.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash | 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter(max_rps: u32, now: Instant) -> Limiter {
        Limiter::new(Some(max_rps), "wss://test.example", now)
    }

    #[test]
    fn a_fresh_limiter_lets_one_call_through_immediately() {
        let now = Instant::now();
        let mut limiter = limiter(20, now);
        assert_eq!(limiter.poll(now), Decision::Ready);
    }

    #[test]
    fn an_empty_bucket_says_how_long_to_wait() {
        let now = Instant::now();
        let mut limiter = limiter(20, now);

        assert_eq!(limiter.poll(now), Decision::Ready);
        // No time has passed, so nothing has refilled.
        let Decision::Wait(wait) = limiter.poll(now) else {
            panic!("expected a wait, got {:?}", limiter.poll(now));
        };
        assert!(wait > Duration::ZERO);

        // Waiting exactly that long makes a token available.
        assert_eq!(limiter.poll(now + wait), Decision::Ready);
    }

    #[test]
    fn success_creeps_the_rate_up_to_the_ceiling_and_no_further() {
        let now = Instant::now();
        let mut limiter = limiter(5, now);
        let start = limiter.rate();

        limiter.succeeded();
        assert!(limiter.rate() > start, "a success should raise the rate");

        for _ in 0..100 {
            limiter.succeeded();
        }
        assert_eq!(
            limiter.rate(),
            5.0,
            "the configured ceiling is a ceiling, not a suggestion"
        );
    }

    #[test]
    fn a_rejection_halves_the_rate() {
        // Additive up, multiplicative down: backing off slowly from a limit already crossed
        // keeps crossing it.
        let now = Instant::now();
        let mut limiter = limiter(20, now);
        for _ in 0..20 {
            limiter.succeeded();
        }

        let before = limiter.rate();
        limiter.failed(now);
        assert!(
            (limiter.rate() - before / 2.0).abs() < f64::EPSILON,
            "expected {before} to halve, got {}",
            limiter.rate()
        );
    }

    #[test]
    fn the_rate_never_falls_below_the_floor() {
        let now = Instant::now();
        let mut limiter = limiter(20, now);
        for _ in 0..50 {
            limiter.failed(now);
        }
        assert_eq!(
            limiter.rate(),
            FLOOR_RPS,
            "an endpoint that keeps failing must still get the occasional try"
        );
    }

    #[test]
    fn the_breaker_opens_after_repeated_failure() {
        let now = Instant::now();
        let mut limiter = limiter(20, now);

        for _ in 0..FAILURES_TO_OPEN - 1 {
            limiter.failed(now);
        }
        assert!(!limiter.is_open(), "one failure short should stay closed");

        limiter.failed(now);
        assert!(limiter.is_open());
        assert!(matches!(limiter.poll(now), Decision::Open(_)));
    }

    #[test]
    fn an_open_breaker_half_opens_after_the_backoff_and_closes_on_a_good_probe() {
        let now = Instant::now();
        let mut limiter = limiter(20, now);
        for _ in 0..FAILURES_TO_OPEN {
            limiter.failed(now);
        }

        let Decision::Open(wait) = limiter.poll(now) else {
            panic!("expected the breaker to be open");
        };

        // Exactly one request is let through, to find out whether the endpoint is back —
        // resuming at full rate would just get us banned again.
        assert_eq!(limiter.poll(now + wait), Decision::Ready);

        limiter.succeeded();
        assert!(!limiter.is_open());
        assert_eq!(
            limiter.rate(),
            FLOOR_RPS,
            "recovery resumes at the floor, not at the rate that caused the outage"
        );
    }

    #[test]
    fn a_failed_probe_reopens_the_breaker_with_a_longer_backoff() {
        let now = Instant::now();
        let mut limiter = limiter(20, now);
        for _ in 0..FAILURES_TO_OPEN {
            limiter.failed(now);
        }

        let Decision::Open(first) = limiter.poll(now) else {
            panic!("expected open");
        };

        // Probe, and fail it.
        assert_eq!(limiter.poll(now + first), Decision::Ready);
        limiter.failed(now + first);

        let Decision::Open(second) = limiter.poll(now + first) else {
            panic!("a failed probe must reopen the breaker, not close it");
        };
        assert!(
            second > first,
            "backoff should double: {second:?} is not longer than {first:?}"
        );
    }

    #[test]
    fn backoff_is_capped() {
        let now = Instant::now();
        let mut limiter = limiter(20, now);

        // Fail probe after probe, forever.
        for _ in 0..FAILURES_TO_OPEN {
            limiter.failed(now);
        }
        let mut at = now;
        for _ in 0..20 {
            let Decision::Open(wait) = limiter.poll(at) else {
                panic!("expected open");
            };
            at += wait;
            assert_eq!(limiter.poll(at), Decision::Ready);
            limiter.failed(at);
        }

        let Decision::Open(wait) = limiter.poll(at) else {
            panic!("expected open");
        };
        assert!(
            wait <= MAX_BACKOFF + Duration::from_millis(1000),
            "unbounded backoff would take an endpoint out for hours: {wait:?}"
        );
    }

    #[test]
    fn an_idle_limiter_does_not_bank_a_burst() {
        // A limiter untouched for an hour must not then permit an hour's worth of calls at
        // once, which is exactly how a well-behaved average rate still gets you banned.
        let now = Instant::now();
        let mut limiter = limiter(20, now);
        for _ in 0..40 {
            limiter.succeeded();
        }

        let later = now + Duration::from_secs(3600);
        let mut burst = 0;
        while limiter.poll(later) == Decision::Ready {
            burst += 1;
            assert!(burst < 1000, "the bucket refilled without bound");
        }
        assert!(
            burst <= 20,
            "an idle hour banked {burst} calls against a 20/s ceiling"
        );
    }

    #[test]
    fn endpoints_do_not_recover_in_lockstep() {
        // Jitter is what stops several endpoints that failed together from all probing on
        // the same tick and hammering whatever they share.
        let now = Instant::now();
        let mut waits = Vec::new();

        for url in ["wss://a.example", "wss://b.example", "wss://c.example"] {
            let mut limiter = Limiter::new(Some(20), url, now);
            for _ in 0..FAILURES_TO_OPEN {
                limiter.failed(now);
            }
            let Decision::Open(wait) = limiter.poll(now) else {
                panic!("expected open");
            };
            waits.push(wait);
        }

        assert!(
            waits.iter().any(|w| *w != waits[0]),
            "every endpoint picked the same backoff: {waits:?}"
        );
    }
}
