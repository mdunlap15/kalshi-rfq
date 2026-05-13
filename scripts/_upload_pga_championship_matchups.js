// One-shot uploader for PGA Championship tournament-matchup lines from
// Bookmaker. Refreshed 2026-05-13 evening for 05/14 tee-off.
// Posts to /betonline-zurich/upload with scope='tournament' so the
// golf-matchup strict-mode pricer (services/pricer.js:~330) reads them
// as authoritative quoting prices.
//
// Usage:
//   AUTH_USERNAME=mike AUTH_PASSWORD=<pw> node scripts/_upload_pga_championship_matchups.js
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

// Pasted from Bookmaker, 2026-05-13 evening for 05/14 tee-off.
// Format: { teamA, oddsA, teamB, oddsB } — moneyline only (spread lines
// are dropped; golf matchups are h2h on PX).
const matchups = [
  // ── First batch: paired-group tournament matchups (various tee times) ──
  { teamA: 'Scottie Scheffler', oddsA: -190, teamB: 'Rory McIlroy', oddsB: +150 },
  { teamA: 'Scottie Scheffler', oddsA: -245, teamB: 'Cameron Young', oddsB: +190 },
  { teamA: 'Rory McIlroy', oddsA: -134, teamB: 'Cameron Young', oddsB: +104 },
  { teamA: 'Jon Rahm', oddsA: -144, teamB: 'Xander Schauffele', oddsB: +114 },
  { teamA: 'Jon Rahm', oddsA: -175, teamB: 'Matt Fitzpatrick', oddsB: +139 },
  { teamA: 'Xander Schauffele', oddsA: -138, teamB: 'Matt Fitzpatrick', oddsB: +108 },
  { teamA: 'Bryson DeChambeau', oddsA: +106, teamB: 'Ludvig Aberg', oddsB: -136 },
  { teamA: 'Bryson DeChambeau', oddsA: -101, teamB: 'Tommy Fleetwood', oddsB: -129 },
  { teamA: 'Ludvig Aberg', oddsA: -126, teamB: 'Tommy Fleetwood', oddsB: -104 },
  { teamA: 'Justin Thomas', oddsA: -127, teamB: 'Brooks Koepka', oddsB: -103 },
  { teamA: 'Justin Thomas', oddsA: -146, teamB: 'Justin Rose', oddsB: +116 },
  { teamA: 'Brooks Koepka', oddsA: -129, teamB: 'Justin Rose', oddsB: -101 },
  { teamA: 'Patrick Cantlay', oddsA: -134, teamB: 'Russell Henley', oddsB: +105 },
  { teamA: 'Patrick Cantlay', oddsA: -127, teamB: 'Si Woo Kim', oddsB: -103 },
  { teamA: 'Russell Henley', oddsA: -103, teamB: 'Si Woo Kim', oddsB: -127 },
  { teamA: 'Collin Morikawa', oddsA: -142, teamB: 'Tyrrell Hatton', oddsB: +112 },
  { teamA: 'Viktor Hovland', oddsA: -123, teamB: 'Chris Gotterup', oddsB: -106 },
  { teamA: 'Sam Burns', oddsA: -101, teamB: 'Robert MacIntyre', oddsB: -128 },
  { teamA: 'Patrick Reed', oddsA: -104, teamB: 'Jordan Spieth', oddsB: -126 },
  { teamA: 'J.J. Spaun', oddsA: -144, teamB: 'Hideki Matsuyama', oddsB: +114 },
  { teamA: 'Sepp Straka', oddsA: -101, teamB: 'Shane Lowry', oddsB: -128 },
  { teamA: 'Rickie Fowler', oddsA: -143, teamB: 'Akshay Bhatia', oddsB: +113 },
  { teamA: 'Kristoffer Reitan', oddsA: +110, teamB: 'Adam Scott', oddsB: -140 },
  { teamA: 'Nicolai Hojgaard', oddsA: -106, teamB: 'Min Woo Lee', oddsB: -124 },
  { teamA: 'Ben Griffin', oddsA: +106, teamB: 'Kurt Kitayama', oddsB: -136 },
  { teamA: 'Joaquin Niemann', oddsA: -139, teamB: 'Gary Woodland', oddsB: +109 },
  { teamA: 'Keegan Bradley', oddsA: -137, teamB: 'Jason Day', oddsB: +107 },
  { teamA: 'Harris English', oddsA: -136, teamB: 'Corey Conners', oddsB: +106 },
  { teamA: 'Jacob Bridgeman', oddsA: -125, teamB: 'Alex Fitzpatrick', oddsB: -105 },
  { teamA: 'Sungjae Im', oddsA: +106, teamB: 'Alex Noren', oddsB: -136 },
  { teamA: 'Marco Penge', oddsA: +117, teamB: 'Wyndham Clark', oddsB: -147 },
  { teamA: 'David Puig', oddsA: -134, teamB: 'Michael Thorbjornsen', oddsB: +104 },
  { teamA: 'Alex Smalley', oddsA: -146, teamB: 'Brian Harman', oddsB: +116 },
  { teamA: 'Thomas Detry', oddsA: -146, teamB: 'Rasmus Hojgaard', oddsB: +116 },
  { teamA: 'Aaron Rai', oddsA: +118, teamB: 'Harry Hall', oddsB: -148 },
  { teamA: 'Matt McCarty', oddsA: -125, teamB: 'Ryan Gerard', oddsB: -105 },
  { teamA: 'Sahith Theegala', oddsA: -115, teamB: 'Daniel Berger', oddsB: -115 },
  { teamA: 'Nick Taylor', oddsA: -153, teamB: 'Max Homa', oddsB: +122 },
  { teamA: 'Sudarshan Yellamaraju', oddsA: -104, teamB: 'Pierceson Coody', oddsB: -126 },
  { teamA: 'Dustin Johnson', oddsA: -140, teamB: 'Cameron Smith', oddsB: +110 },
  { teamA: 'Michael Brennan', oddsA: -101, teamB: 'Sam Stevens', oddsB: -129 },
  { teamA: 'Aldrich Potgieter', oddsA: +112, teamB: 'Ryan Fox', oddsB: -142 },
  { teamA: 'Keith Mitchell', oddsA: -119, teamB: 'Bud Cauley', oddsB: -111 },
  { teamA: 'Angel Ayora', oddsA: -131, teamB: 'Jayden Schaper', oddsB: +101 },
  { teamA: 'J.T. Poston', oddsA: -102, teamB: 'Jordan Smith', oddsB: -128 },
  { teamA: 'Tom McKibbin', oddsA: -145, teamB: 'Haotong Li', oddsB: +115 },
  { teamA: 'Denny McCarthy', oddsA: -102, teamB: 'Taylor Pendrith', oddsB: -128 },
  { teamA: 'Matt Wallace', oddsA: -140, teamB: 'Daniel Hillier', oddsB: +110 },
  { teamA: 'Rasmus Neergaard-Petersen', oddsA: -105, teamB: 'Christiaan Bezuidenhout', oddsB: -125 },
  { teamA: 'Ryo Hisatsune', oddsA: -136, teamB: 'Michael Kim', oddsB: +106 },
  { teamA: 'Andrew Novak', oddsA: -170, teamB: 'Bernd Wiesberger', oddsB: +135 },
  { teamA: 'Lucas Glover', oddsA: -125, teamB: 'Billy Horschel', oddsB: -105 },
  { teamA: 'Max Greyserman', oddsA: -120, teamB: 'Patrick Rodgers', oddsB: -110 },
  { teamA: 'Austin Smotherman', oddsA: -114, teamB: 'Rico Hoey', oddsB: -116 },
  { teamA: 'John Parry', oddsA: -105, teamB: 'Andrew Putnam', oddsB: -125 },
  { teamA: 'Mikael Lindberg', oddsA: -120, teamB: 'Casey Jarvis', oddsB: -110 },
  { teamA: 'Nico Echavarria', oddsA: -108, teamB: 'Sami Valimaki', oddsB: -122 },
  { teamA: 'Matti Schmid', oddsA: -172, teamB: 'Elvis Smylie', oddsB: +137 },
  { teamA: 'Ricky Castillo', oddsA: -138, teamB: 'Stewart Cink', oddsB: +108 },

  // ── Second batch: additional cross-pair tournament matchups (Bookmaker 05/14 05:00 listing) ──
  { teamA: 'Alex Noren', oddsA: -128, teamB: 'Alex Smalley', oddsB: -102 },
  { teamA: 'Collin Morikawa', oddsA: -121, teamB: 'Brooks Koepka', oddsB: -109 },
  { teamA: 'Jayden Schaper', oddsA: -111, teamB: 'Michael Kim', oddsB: -119 },
  { teamA: 'David Puig', oddsA: -127, teamB: 'Wyndham Clark', oddsB: -103 },
  { teamA: 'Angel Ayora', oddsA: -120, teamB: 'Tom McKibbin', oddsB: -110 },
  { teamA: 'Dustin Johnson', oddsA: -116, teamB: 'Billy Horschel', oddsB: -114 },
  { teamA: 'Jason Day', oddsA: -101, teamB: 'Harry Hall', oddsB: -129 },
  { teamA: 'Daniel Berger', oddsA: -146, teamB: 'Max Homa', oddsB: +116 },
  { teamA: 'Nick Taylor', oddsA: -114, teamB: 'Sungjae Im', oddsB: -116 },
  { teamA: 'Justin Rose', oddsA: +130, teamB: 'Robert MacIntyre', oddsB: -163 },
  { teamA: 'Tommy Fleetwood', oddsA: -121, teamB: 'Matt Fitzpatrick', oddsB: -109 },
  { teamA: 'Ricky Castillo', oddsA: -111, teamB: 'Daniel Hillier', oddsB: -119 },
  { teamA: 'Joaquin Niemann', oddsA: -117, teamB: 'Harris English', oddsB: -113 },
  { teamA: 'Justin Thomas', oddsA: -113, teamB: 'Russell Henley', oddsB: -117 },
  { teamA: 'Brian Harman', oddsA: -110, teamB: 'Aaron Rai', oddsB: -120 },
  { teamA: 'Hideki Matsuyama', oddsA: -102, teamB: 'Tyrrell Hatton', oddsB: -128 },
  { teamA: 'Bryson DeChambeau', oddsA: -115, teamB: 'Matt Fitzpatrick', oddsB: -115 },
  { teamA: 'Chris Gotterup', oddsA: -106, teamB: 'Nicolai Hojgaard', oddsB: -124 },
  { teamA: 'Rickie Fowler', oddsA: -118, teamB: 'J.J. Spaun', oddsB: -112 },
  { teamA: 'Justin Rose', oddsA: +131, teamB: 'Rickie Fowler', oddsB: -165 },
  { teamA: 'Corey Conners', oddsA: -109, teamB: 'Ryan Gerard', oddsB: -121 },
  { teamA: 'Tyrrell Hatton', oddsA: +100, teamB: 'Viktor Hovland', oddsB: -130 },
  { teamA: 'Sam Burns', oddsA: -128, teamB: 'Maverick McNealy', oddsB: -102 },
  { teamA: 'Pierceson Coody', oddsA: +104, teamB: 'Ryo Hisatsune', oddsB: -134 },
  { teamA: 'Keith Mitchell', oddsA: -125, teamB: 'Sahith Theegala', oddsB: -105 },
  { teamA: 'Min Woo Lee', oddsA: -132, teamB: 'Adam Scott', oddsB: +102 },
];

(async () => {
  console.log('Uploading ' + matchups.length + ' tournament-matchup pairings...');
  const auth = 'Basic ' + Buffer.from(USER + ':' + PASS).toString('base64');
  const resp = await fetch(URL + '/betonline-zurich/upload', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: auth },
    body: JSON.stringify({ scope: 'tournament', matchups }),
  });
  const body = await resp.json().catch(() => ({}));
  console.log('Status: ' + resp.status);
  console.log(JSON.stringify(body, null, 2));
  if (!resp.ok) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
