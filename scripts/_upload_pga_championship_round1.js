// One-shot uploader for PGA Championship ROUND 1 matchup lines from
// Bookmaker. Pasted 2026-05-13 evening for 05/14 R1 tee-off.
// Posts to /betonline-zurich/upload with scope='round_1' so the
// golf-matchup pricer reads them as authoritative R1-only quoting
// prices (separate from the tournament-length matchups uploaded by
// _upload_pga_championship_matchups.js).
//
// Usage:
//   AUTH_USERNAME=mike AUTH_PASSWORD=<pw> node scripts/_upload_pga_championship_round1.js
//
// Or set the creds in .env. URL override via PROD_URL env var.

require('dotenv').config();

const URL = process.env.PROD_URL || 'https://prophetx-rfq-production-6781.up.railway.app';
const USER = process.env.AUTH_USERNAME;
const PASS = process.env.AUTH_PASSWORD;

if (!USER || !PASS) {
  console.error('AUTH_USERNAME and AUTH_PASSWORD must be set (in env or .env)');
  process.exit(1);
}

// Pasted from Bookmaker, 2026-05-13 evening for 05/14 R1 tee-off.
// Names normalized to match the tournament file's canonical spelling
// where the same player appears in both lists (e.g. McKibbin,
// DeChambeau, Echavarria, Neergaard-Petersen).
const matchups = [
  { teamA: 'Ben Griffin', oddsA: -139, teamB: 'Alex Fitzpatrick', oddsB: +109 },
  { teamA: 'Harry Hall', oddsA: -133, teamB: 'Ryan Gerard', oddsB: +103 },
  { teamA: 'Rico Hoey', oddsA: -129, teamB: 'Johnny Keefer', oddsB: -101 },
  { teamA: 'Nicolai Hojgaard', oddsA: -163, teamB: 'Michael Brennan', oddsB: +130 },
  { teamA: 'Garrick Higgo', oddsA: +101, teamB: 'Jhonattan Vegas', oddsB: -131 },
  { teamA: 'Matt McCarty', oddsA: -135, teamB: 'Tom McKibbin', oddsB: +105 },
  { teamA: 'Lucas Glover', oddsA: -123, teamB: 'Stephan Jaeger', oddsB: -107 },
  { teamA: 'Daniel Brown', oddsA: -122, teamB: 'Adrien Saddier', oddsB: -108 },
  { teamA: 'Harris English', oddsA: -117, teamB: 'Jacob Bridgeman', oddsB: -113 },
  { teamA: 'Alex Noren', oddsA: -108, teamB: 'Kristoffer Reitan', oddsB: -122 },
  { teamA: 'Max Greyserman', oddsA: -112, teamB: 'Chris Kirk', oddsB: -118 },
  { teamA: 'Maverick McNealy', oddsA: -126, teamB: 'Thomas Detry', oddsB: -104 },
  { teamA: 'Bud Cauley', oddsA: -124, teamB: 'Ryan Fox', oddsB: -106 },
  { teamA: 'Ryo Hisatsune', oddsA: -136, teamB: 'Michael Kim', oddsB: +106 },
  { teamA: 'Andrew Novak', oddsA: -135, teamB: 'John Parry', oddsB: +105 },
  { teamA: 'Kurt Kitayama', oddsA: -140, teamB: 'Akshay Bhatia', oddsB: +110 },
  { teamA: 'Michael Thorbjornsen', oddsA: -133, teamB: 'Ricky Castillo', oddsB: +103 },
  { teamA: 'Nico Echavarria', oddsA: -109, teamB: 'Stewart Cink', oddsB: -121 },
  { teamA: 'J.J. Spaun', oddsA: -129, teamB: 'Hideki Matsuyama', oddsB: -101 },
  { teamA: 'Adam Scott', oddsA: -144, teamB: 'Corey Conners', oddsB: +114 },
  { teamA: 'Russell Henley', oddsA: -154, teamB: 'Daniel Berger', oddsB: +123 },
  { teamA: 'Max Homa', oddsA: -104, teamB: 'J.T. Poston', oddsB: -126 },
  { teamA: 'Collin Morikawa', oddsA: -116, teamB: 'Viktor Hovland', oddsB: -114 },
  { teamA: 'Tommy Fleetwood', oddsA: -140, teamB: 'Robert MacIntyre', oddsB: +110 },
  { teamA: 'Scottie Scheffler', oddsA: -182, teamB: 'Cameron Young', oddsB: +144 },
  { teamA: 'Justin Thomas', oddsA: -128, teamB: 'Keegan Bradley', oddsB: -102 },
  { teamA: 'Matt Fitzpatrick', oddsA: -170, teamB: 'Justin Rose', oddsB: +135 },
  { teamA: 'Shane Lowry', oddsA: -106, teamB: 'Chris Gotterup', oddsB: -124 },
  { teamA: 'Alex Smalley', oddsA: -129, teamB: 'Sudarshan Yellamaraju', oddsB: -101 },
  { teamA: 'Jordan Smith', oddsA: -113, teamB: 'Nick Taylor', oddsB: -117 },
  { teamA: 'Haotong Li', oddsA: -105, teamB: 'Rasmus Neergaard-Petersen', oddsB: -125 },
  { teamA: 'Pierceson Coody', oddsA: -111, teamB: 'Patrick Reed', oddsB: -119 },
  { teamA: 'Emiliano Grillo', oddsA: +121, teamB: 'Christiaan Bezuidenhout', oddsB: -152 },
  { teamA: 'Sungjae Im', oddsA: -130, teamB: 'Rasmus Hojgaard', oddsB: +100 },
  { teamA: 'Marco Penge', oddsA: -112, teamB: 'Patrick Rodgers', oddsB: -118 },
  { teamA: 'Dustin Johnson', oddsA: -110, teamB: 'Steven Fisk', oddsB: -120 },
  { teamA: 'David Lipsky', oddsA: -101, teamB: 'Casey Jarvis', oddsB: -129 },
  { teamA: 'Matt Wallace', oddsA: -128, teamB: 'Andrew Putnam', oddsB: -102 },
  { teamA: 'Sepp Straka', oddsA: -122, teamB: 'Aaron Rai', oddsB: -108 },
  { teamA: 'Sam Stevens', oddsA: -142, teamB: 'Jayden Schaper', oddsB: +112 },
  { teamA: 'Matti Schmid', oddsA: -103, teamB: 'Austin Smotherman', oddsB: -127 },
  { teamA: 'Aldrich Potgieter', oddsA: -104, teamB: 'Denny McCarthy', oddsB: -126 },
  { teamA: 'David Puig', oddsA: -125, teamB: 'Taylor Pendrith', oddsB: -105 },
  { teamA: 'Joaquin Niemann', oddsA: -139, teamB: 'Keith Mitchell', oddsB: +109 },
  { teamA: 'Sam Burns', oddsA: -137, teamB: 'Jason Day', oddsB: +107 },
  { teamA: 'Wyndham Clark', oddsA: -124, teamB: 'Brian Harman', oddsB: -106 },
  { teamA: 'Min Woo Lee', oddsA: -107, teamB: 'Patrick Cantlay', oddsB: -123 },
  { teamA: 'Gary Woodland', oddsA: -134, teamB: 'Sahith Theegala', oddsB: +104 },
  { teamA: 'Cameron Smith', oddsA: -102, teamB: 'Billy Horschel', oddsB: -128 },
  { teamA: 'Ludvig Aberg', oddsA: -121, teamB: 'Bryson DeChambeau', oddsB: -109 },
  { teamA: 'Si Woo Kim', oddsA: -125, teamB: 'Rickie Fowler', oddsB: -105 },
  { teamA: 'Rory McIlroy', oddsA: -110, teamB: 'Jon Rahm', oddsB: -120 },
  { teamA: 'Brooks Koepka', oddsA: -119, teamB: 'Tyrrell Hatton', oddsB: -111 },
  { teamA: 'Xander Schauffele', oddsA: -158, teamB: 'Jordan Spieth', oddsB: +126 },
];

(async () => {
  console.log('Uploading ' + matchups.length + ' Round 1 matchup pairings...');
  const auth = 'Basic ' + Buffer.from(USER + ':' + PASS).toString('base64');
  const resp = await fetch(URL + '/betonline-zurich/upload', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: auth },
    body: JSON.stringify({ scope: 'round_1', matchups }),
  });
  const body = await resp.json().catch(() => ({}));
  console.log('Status: ' + resp.status);
  console.log(JSON.stringify(body, null, 2));
  if (!resp.ok) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
