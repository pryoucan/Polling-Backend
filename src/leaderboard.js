import { query } from './db.js';

// Top-N leaderboard, read straight from the authoritative score table.
// Cheap thanks to idx_score_leaderboard.
// `full` (host-only) returns the complete phone number; otherwise only the last
// 4 digits as `tag`, which is public-safe and lets a participant spot themselves.
export async function getLeaderboard(pollId, limit = 20, full = false) {
  const phoneSel = full ? 'p.phone AS phone' : "right(p.phone, 4) AS tag";
  const { rows } = await query(
    `SELECT s.participant_id AS id, p.display_name AS name, s.points, ${phoneSel}
       FROM score s
       JOIN participant p ON p.id = s.participant_id
      WHERE s.poll_id = $1
      ORDER BY s.points DESC, p.display_name ASC
      LIMIT $2`,
    [pollId, limit],
  );
  return rows.map((r, i) => ({
    rank: i + 1,
    id: r.id,
    name: r.name,
    points: r.points,
    ...(full ? { phone: r.phone } : { tag: r.tag }),
  }));
}

// Segment leaderboard: standings over a contiguous block of question positions
// (e.g. Q1-10 => fromPos 0, toPos 9). Summed from question_score, so it needs no
// reset and every past segment stays queryable. Returns [] before any question
// in the range has closed (i.e. a fresh segment shows empty).
export async function getSegmentLeaderboard(pollId, fromPos, toPos, limit = 20, full = false) {
  const phoneSel = full ? 'p.phone AS phone' : "right(p.phone, 4) AS tag";
  const { rows } = await query(
    `SELECT qs.participant_id AS id, p.display_name AS name, SUM(qs.points)::int AS points, ${phoneSel}
       FROM question_score qs
       JOIN question q     ON q.id = qs.question_id
       JOIN participant p  ON p.id = qs.participant_id
      WHERE q.poll_id = $1 AND q.position BETWEEN $2 AND $3
      GROUP BY qs.participant_id, p.display_name, p.phone
      ORDER BY points DESC, p.display_name ASC
      LIMIT $4`,
    [pollId, fromPos, toPos, limit],
  );
  return rows.map((r, i) => ({
    rank: i + 1,
    id: r.id,
    name: r.name,
    points: r.points,
    ...(full ? { phone: r.phone } : { tag: r.tag }),
  }));
}

// A single participant's points + rank (for showing "you are #142").
export async function getParticipantStanding(pollId, participantId) {
  const { rows } = await query(
    `SELECT points,
            (SELECT COUNT(*) + 1 FROM score s2
              WHERE s2.poll_id = $1 AND s2.points > s.points) AS rank
       FROM score s
      WHERE s.poll_id = $1 AND s.participant_id = $2`,
    [pollId, participantId],
  );
  if (rows.length === 0) return { points: 0, rank: null };
  return { points: rows[0].points, rank: Number(rows[0].rank) };
}
