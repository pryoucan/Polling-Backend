import { query } from './db.js';

// Top-N leaderboard, read straight from the authoritative score table.
// Cheap thanks to idx_score_leaderboard.
export async function getLeaderboard(pollId, limit = 20) {
  const { rows } = await query(
    `SELECT s.participant_id AS id, p.display_name AS name, s.points
       FROM score s
       JOIN participant p ON p.id = s.participant_id
      WHERE s.poll_id = $1
      ORDER BY s.points DESC, p.display_name ASC
      LIMIT $2`,
    [pollId, limit],
  );
  return rows.map((r, i) => ({ rank: i + 1, id: r.id, name: r.name, points: r.points }));
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
