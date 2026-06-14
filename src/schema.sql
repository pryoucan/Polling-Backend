-- Schema for the live timed polling app.
-- Postgres is the source of truth; Redis is only a fast live cache.

CREATE TABLE IF NOT EXISTS poll (
    id          SERIAL PRIMARY KEY,
    title       TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending | running | complete
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS question (
    id          SERIAL PRIMARY KEY,
    poll_id     INT  NOT NULL REFERENCES poll(id) ON DELETE CASCADE,
    prompt      TEXT NOT NULL,                     -- e.g. "Fruits", "Beautiful Indian Places"
    position    INT  NOT NULL,                     -- 0-based order within the poll
    min_select  INT  NOT NULL DEFAULT 1,
    max_select  INT  NOT NULL DEFAULT 5,
    UNIQUE (poll_id, position)
);

CREATE TABLE IF NOT EXISTS option (
    id          SERIAL PRIMARY KEY,
    question_id INT  NOT NULL REFERENCES question(id) ON DELETE CASCADE,
    label       TEXT NOT NULL,
    position    INT  NOT NULL,
    UNIQUE (question_id, position)
);

CREATE TABLE IF NOT EXISTS participant (
    id            UUID PRIMARY KEY,                -- == signed session token
    display_name  TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Speeds up the /join duplicate-name check. That query does both an equality
-- match (lower(display_name) = ...) and a prefix LIKE (... LIKE 'name (%'); the
-- text_pattern_ops opclass serves BOTH, so the check becomes an index probe
-- instead of a full table scan on every single join. Without this, a burst of
-- thousands of joins is O(n^2) and pegs the DB CPU (the load-test hot spot).
CREATE INDEX IF NOT EXISTS idx_participant_lower_name
    ON participant (lower(display_name) text_pattern_ops);

-- One row guarantees a participant can only submit ONCE per question (atomic guard).
CREATE TABLE IF NOT EXISTS question_submission (
    participant_id UUID NOT NULL REFERENCES participant(id) ON DELETE CASCADE,
    question_id    INT  NOT NULL REFERENCES question(id) ON DELETE CASCADE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (participant_id, question_id)
);

-- One row per selected option.
CREATE TABLE IF NOT EXISTS vote (
    participant_id UUID NOT NULL REFERENCES participant(id) ON DELETE CASCADE,
    question_id    INT  NOT NULL REFERENCES question(id) ON DELETE CASCADE,
    option_id      INT  NOT NULL REFERENCES option(id) ON DELETE CASCADE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (participant_id, option_id)
);

-- Powers the authoritative per-question tally (GROUP BY option_id) at close time.
CREATE INDEX IF NOT EXISTS idx_vote_question ON vote(question_id);

-- The vote PK is (participant_id, option_id), so option_id can't be looked up via
-- the PK. But closeQuestion filters votes BY option_id twice — the tally join
-- (v.option_id = o.id) and the per-winner scoring (WHERE v.option_id = $1).
-- Without this index those are full scans of the (large) vote table on every
-- question close. Indexing option_id turns them into index lookups.
CREATE INDEX IF NOT EXISTS idx_vote_option ON vote(option_id);
 
-- Final settled leaderboard. Written incrementally as each question closes.
CREATE TABLE IF NOT EXISTS score (
    poll_id        INT  NOT NULL REFERENCES poll(id) ON DELETE CASCADE,
    participant_id UUID NOT NULL REFERENCES participant(id) ON DELETE CASCADE,
    points         INT  NOT NULL DEFAULT 0,
    PRIMARY KEY (poll_id, participant_id)
);

-- Fast top-N leaderboard reads.
CREATE INDEX IF NOT EXISTS idx_score_leaderboard ON score(poll_id, points DESC);

-- Frozen result of each question (top options + counts) for audit/replay.
CREATE TABLE IF NOT EXISTS question_result (
    question_id INT  PRIMARY KEY REFERENCES question(id) ON DELETE CASCADE,
    tally       JSONB NOT NULL,   -- [{optionId, label, votes, rank}]
    closed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
