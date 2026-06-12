// Anonymous identity: a server-signed UUID stored in a cookie. No password.
// The signature stops anyone from forging/swapping someone else's id.
import crypto from 'node:crypto';
import { config } from './config.js';

const COOKIE = 'pid';

function sign(id) {
  const mac = crypto.createHmac('sha256', config.sessionSecret).update(id).digest('hex');
  return `${id}.${mac}`;
}

export function makeToken() {
  return sign(crypto.randomUUID());
}

// Returns the participant id if the token is valid, else null.
export function verifyToken(token) {
  if (!token || typeof token !== 'string') return null;
  const dot = token.lastIndexOf('.');
  if (dot < 0) return null;
  const id = token.slice(0, dot);
  const mac = token.slice(dot + 1);
  const expected = crypto
    .createHmac('sha256', config.sessionSecret)
    .update(id)
    .digest('hex');
  // Constant-time compare.
  const a = Buffer.from(mac);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
  return id;
}

function parseCookies(header) {
  const out = {};
  if (!header) return out;
  for (const part of header.split(';')) {
    const i = part.indexOf('=');
    if (i < 0) continue;
    out[part.slice(0, i).trim()] = decodeURIComponent(part.slice(i + 1).trim());
  }
  return out;
}

// Express middleware: attaches req.participantId (or null).
// Accepts the token two ways:
//   1. Authorization: Bearer <token>  — used by the cross-origin React client
//   2. the `pid` cookie               — used by a same-origin client
export function withParticipant(req, _res, next) {
  let token = null;
  const auth = req.headers.authorization;
  if (auth && auth.startsWith('Bearer ')) {
    token = auth.slice(7).trim();
  } else {
    token = parseCookies(req.headers.cookie)[COOKIE];
  }
  req.participantId = verifyToken(token);
  next();
}

export function setSessionCookie(res, token) {
  const secure = process.env.NODE_ENV === 'production';
  res.cookie(COOKIE, token, {
    httpOnly: true,
    sameSite: 'lax',
    secure,
    maxAge: 24 * 60 * 60 * 1000, // 1 day — long enough for one event
    path: '/',
  });
}

export { COOKIE };
