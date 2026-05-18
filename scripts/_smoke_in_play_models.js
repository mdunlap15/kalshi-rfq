// Smoke test for the in-play model. Walks through realistic states and
// prints the live-prob output so we can sanity check the math.
const m = require('../services/in-play-models');

function show(label, leg, state) {
  const r = m.computeLiveLegProb(leg, state);
  const bettor = r.liveFairProb;
  const sp = bettor != null ? 1 - bettor : null;
  console.log(label.padEnd(70), '→',
    'bettor=' + (bettor != null ? bettor.toFixed(3) : 'null'),
    'SP=' + (sp != null ? sp.toFixed(3) : 'null'),
    `conf=${r.confidence}`, `(${r.reason})`);
}

console.log('=== WNBA spread legs (the real-world case) ===');
// Atlanta Dream took +3.5 (bettor took home +3.5)
// Late in Q4 with Atlanta down 30 → bettor's leg dead → SP wins (prob → 1)
show('Pre-game Atlanta +3.5',
  { sport: 'basketball_wnba', market: 'spread', selection: 'home', line: 3.5, fairProb: 0.50, homeTeam: 'Atlanta', awayTeam: 'Las Vegas' },
  { state: 'pre', period: 0, homeScore: 0, awayScore: 0 });
show('Atlanta down 30, 4:00 in Q4 (Atlanta +3.5)',
  { sport: 'basketball_wnba', market: 'spread', selection: 'home', line: 3.5, fairProb: 0.50, homeTeam: 'Atlanta', awayTeam: 'Las Vegas' },
  { state: 'in', period: 4, displayClock: '4:00', homeScore: 30, awayScore: 60 });
show('Atlanta down 4, 4:00 in Q4 (Atlanta +3.5) — pick-your-poison',
  { sport: 'basketball_wnba', market: 'spread', selection: 'home', line: 3.5, fairProb: 0.50, homeTeam: 'Atlanta', awayTeam: 'Las Vegas' },
  { state: 'in', period: 4, displayClock: '4:00', homeScore: 50, awayScore: 54 });
show('Final: Atlanta lost 60-90 (Atlanta +3.5)',
  { sport: 'basketball_wnba', market: 'spread', selection: 'home', line: 3.5, fairProb: 0.50, homeTeam: 'Atlanta', awayTeam: 'Las Vegas' },
  { state: 'post', completed: true, period: 4, homeScore: 60, awayScore: 90 });

console.log('\n=== NBA moneyline ===');
show('Pre-game: Pistons ML (bettor took home)',
  { sport: 'basketball_nba', market: 'moneyline', selection: 'home', fairProb: 0.62, homeTeam: 'Detroit', awayTeam: 'Cleveland' },
  { state: 'pre', period: 0, homeScore: 0, awayScore: 0 });
show('Pistons up 10, 5:00 in Q4',
  { sport: 'basketball_nba', market: 'moneyline', selection: 'home', fairProb: 0.62, homeTeam: 'Detroit', awayTeam: 'Cleveland' },
  { state: 'in', period: 4, displayClock: '5:00', homeScore: 95, awayScore: 85 });
show('Pistons up 20, 8:00 in Q4',
  { sport: 'basketball_nba', market: 'moneyline', selection: 'home', fairProb: 0.62, homeTeam: 'Detroit', awayTeam: 'Cleveland' },
  { state: 'in', period: 4, displayClock: '8:00', homeScore: 95, awayScore: 75 });
show('Pistons down 5, 2:00 in Q4',
  { sport: 'basketball_nba', market: 'moneyline', selection: 'home', fairProb: 0.62, homeTeam: 'Detroit', awayTeam: 'Cleveland' },
  { state: 'in', period: 4, displayClock: '2:00', homeScore: 95, awayScore: 100 });

console.log('\n=== MLB — bug-fix regression (period=null + Top/Mid/End/Bot parsing) ===');
// Mike's reported case: SEA Mariners ML (bettor took home), SD 8 SEA 3, Top 8th
// DK live ML for SEA was +2900 (~3% implied) — bettor near-dead.
show("SEA ML bettor, SD 8 @ SEA 3, Top 8th (period=null)",
  { sport: 'baseball_mlb', market: 'moneyline', selection: 'home', fairProb: 0.60, homeTeam: 'Seattle Mariners', awayTeam: 'San Diego Padres' },
  { state: 'in', period: null, displayClock: 'Top 8th', shortDetail: 'Top 8th', homeScore: 3, awayScore: 8 });
show("SEA ML bettor, same state but period=8 set",
  { sport: 'baseball_mlb', market: 'moneyline', selection: 'home', fairProb: 0.60, homeTeam: 'Seattle Mariners', awayTeam: 'San Diego Padres' },
  { state: 'in', period: 8, displayClock: 'Top 8th', shortDetail: 'Top 8th', homeScore: 3, awayScore: 8 });
show("MLB Mid 7th (between halves)",
  { sport: 'baseball_mlb', market: 'moneyline', selection: 'home', fairProb: 0.55, homeTeam: 'X', awayTeam: 'Y' },
  { state: 'in', period: null, displayClock: 'Mid 7th', shortDetail: 'Mid 7th', homeScore: 4, awayScore: 6 });
show("MLB End 7th",
  { sport: 'baseball_mlb', market: 'moneyline', selection: 'home', fairProb: 0.55, homeTeam: 'X', awayTeam: 'Y' },
  { state: 'in', period: null, displayClock: 'End 7th', shortDetail: 'End 7th', homeScore: 6, awayScore: 4 });

console.log('\n=== NBA / WNBA halftime (period=null) ===');
show('Cavs/Pistons Halftime, Pistons home down 17 (Cavs/Pistons leg)',
  { sport: 'basketball_nba', market: 'moneyline', selection: 'home', fairProb: 0.40, homeTeam: 'Detroit Pistons', awayTeam: 'Cleveland Cavaliers' },
  { state: 'in', period: null, displayClock: 'Halftime', shortDetail: 'Halftime', homeScore: 47, awayScore: 64 });

console.log('\n=== MLB ===');
show('Bot 7th, home up 4 runs (away ML)',
  { sport: 'baseball_mlb', market: 'moneyline', selection: 'away', fairProb: 0.45, homeTeam: 'Phillies', awayTeam: 'Mets' },
  { state: 'in', period: 7, shortDetail: 'Bot 7th', homeScore: 6, awayScore: 2 });
show('Top 9th, home up 1 (home ML)',
  { sport: 'baseball_mlb', market: 'moneyline', selection: 'home', fairProb: 0.55, homeTeam: 'Yankees', awayTeam: 'Rays' },
  { state: 'in', period: 9, shortDetail: 'Top 9th', homeScore: 4, awayScore: 3 });
show('Bot 5th, total at 8.5, current 5 runs (over)',
  { sport: 'baseball_mlb', market: 'total', selection: 'over', line: 8.5, fairProb: 0.50, homeTeam: 'X', awayTeam: 'Y' },
  { state: 'in', period: 5, shortDetail: 'Bot 5th', homeScore: 3, awayScore: 2 });

console.log('\n=== NHL ===');
show('Period 3, 5:00 left, home up 2',
  { sport: 'icehockey_nhl', market: 'moneyline', selection: 'home', fairProb: 0.55 },
  { state: 'in', period: 3, displayClock: '5:00', homeScore: 4, awayScore: 2 });

console.log('\n=== Sports without a model (tennis/soccer) — falls back to static ===');
show('Pre-game tennis',
  { sport: 'tennis', market: 'moneyline', selection: 'home', fairProb: 0.65 },
  { state: 'pre', period: 0, homeScore: 0, awayScore: 0 });

console.log('\n=== Parlay aggregation ===');
const legResults = [
  { liveFairProb: 0.99, confidence: 'high' },
  { liveFairProb: 0.85, confidence: 'medium' },
];
const parlay = m.computeLiveParlayProb(legResults);
console.log('  2-leg parlay (0.99 × 0.85)  →', JSON.stringify(parlay));
const deadLeg = m.computeLiveParlayProb([
  { liveFairProb: 0.0, confidence: 'final' },
  { liveFairProb: 0.85, confidence: 'medium' },
]);
console.log('  with one dead leg            →', JSON.stringify({ liveFairProb: deadLeg.liveFairProb, anyDead: deadLeg.anyDead, allFinal: deadLeg.allFinal }));
