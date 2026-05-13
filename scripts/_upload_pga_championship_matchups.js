// One-shot uploader for PGA Championship tournament-matchup lines from
// Bookmaker (2026-05-14 tee-off). Posts to /betonline-zurich/upload
// with scope='tournament' so the golf-matchup strict-mode pricer
// (services/pricer.js:~330) reads them as authoritative quoting prices.
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

// Pasted from Bookmaker, 2026-05-13 evening for 05/14 05:00 tee-off.
// Format: { teamA, oddsA, teamB, oddsB } — moneyline only (spread lines
// are dropped; golf matchups are h2h on PX).
const matchups = [
  { teamA: 'Scottie Scheffler', oddsA: -196, teamB: 'Rory McIlroy', oddsB: +155 },
  { teamA: 'Scottie Scheffler', oddsA: -245, teamB: 'Cameron Young', oddsB: +190 },
  { teamA: 'Rory McIlroy', oddsA: -134, teamB: 'Cameron Young', oddsB: +104 },
  { teamA: 'Jon Rahm', oddsA: -144, teamB: 'Xander Schauffele', oddsB: +114 },
  { teamA: 'Jon Rahm', oddsA: -180, teamB: 'Matt Fitzpatrick', oddsB: +143 },
  { teamA: 'Xander Schauffele', oddsA: -138, teamB: 'Matt Fitzpatrick', oddsB: +108 },
  { teamA: 'Bryson DeChambeau', oddsA: -104, teamB: 'Ludvig Aberg', oddsB: -126 },
  { teamA: 'Bryson DeChambeau', oddsA: -101, teamB: 'Tommy Fleetwood', oddsB: -129 },
  { teamA: 'Ludvig Aberg', oddsA: -126, teamB: 'Tommy Fleetwood', oddsB: -104 },
  { teamA: 'Justin Thomas', oddsA: -127, teamB: 'Brooks Koepka', oddsB: -103 },
  { teamA: 'Justin Thomas', oddsA: -146, teamB: 'Justin Rose', oddsB: +116 },
  { teamA: 'Brooks Koepka', oddsA: -129, teamB: 'Justin Rose', oddsB: -101 },
  { teamA: 'Patrick Cantlay', oddsA: -137, teamB: 'Russell Henley', oddsB: +107 },
  { teamA: 'Patrick Cantlay', oddsA: -125, teamB: 'Si Woo Kim', oddsB: -105 },
  { teamA: 'Russell Henley', oddsA: -103, teamB: 'Si Woo Kim', oddsB: -127 },
  { teamA: 'Collin Morikawa', oddsA: -145, teamB: 'Tyrrell Hatton', oddsB: +115 },
  { teamA: 'Viktor Hovland', oddsA: -127, teamB: 'Chris Gotterup', oddsB: -103 },
  { teamA: 'Sam Burns', oddsA: +100, teamB: 'Robert MacIntyre', oddsB: -130 },
  { teamA: 'Patrick Reed', oddsA: +106, teamB: 'Jordan Spieth', oddsB: -136 },
  { teamA: 'J.J. Spaun', oddsA: -144, teamB: 'Hideki Matsuyama', oddsB: +114 },
  { teamA: 'Sepp Straka', oddsA: +102, teamB: 'Shane Lowry', oddsB: -132 },
  { teamA: 'Rickie Fowler', oddsA: -143, teamB: 'Akshay Bhatia', oddsB: +113 },
  { teamA: 'Kristoffer Reitan', oddsA: +110, teamB: 'Adam Scott', oddsB: -140 },
  { teamA: 'Nicolai Hojgaard', oddsA: -106, teamB: 'Min Woo Lee', oddsB: -124 },
  { teamA: 'Ben Griffin', oddsA: +106, teamB: 'Kurt Kitayama', oddsB: -136 },
  { teamA: 'Joaquin Niemann', oddsA: -139, teamB: 'Gary Woodland', oddsB: +109 },
  { teamA: 'Keegan Bradley', oddsA: -137, teamB: 'Jason Day', oddsB: +107 },
  { teamA: 'Harris English', oddsA: -131, teamB: 'Corey Conners', oddsB: +101 },
  { teamA: 'Jacob Bridgeman', oddsA: -135, teamB: 'Alex Fitzpatrick', oddsB: +105 },
  { teamA: 'Sungjae Im', oddsA: +106, teamB: 'Alex Noren', oddsB: -136 },
  { teamA: 'Marco Penge', oddsA: +121, teamB: 'Wyndham Clark', oddsB: -152 },
  { teamA: 'David Puig', oddsA: -134, teamB: 'Michael Thorbjornsen', oddsB: +104 },
  { teamA: 'Alex Smalley', oddsA: -146, teamB: 'Brian Harman', oddsB: +116 },
  { teamA: 'Thomas Detry', oddsA: -146, teamB: 'Rasmus Hojgaard', oddsB: +116 },
  { teamA: 'Aaron Rai', oddsA: +118, teamB: 'Harry Hall', oddsB: -148 },
  { teamA: 'Matt McCarty', oddsA: -125, teamB: 'Ryan Gerard', oddsB: -105 },
  { teamA: 'Sahith Theegala', oddsA: -102, teamB: 'Daniel Berger', oddsB: -128 },
  { teamA: 'Nick Taylor', oddsA: -153, teamB: 'Max Homa', oddsB: +122 },
  { teamA: 'Sudarshan Yellamaraju', oddsA: -104, teamB: 'Pierceson Coody', oddsB: -126 },
  { teamA: 'Dustin Johnson', oddsA: -146, teamB: 'Cameron Smith', oddsB: +116 },
  { teamA: 'Michael Brennan', oddsA: +111, teamB: 'Sam Stevens', oddsB: -141 },
  { teamA: 'Aldrich Potgieter', oddsA: +102, teamB: 'Ryan Fox', oddsB: -132 },
  { teamA: 'Keith Mitchell', oddsA: -119, teamB: 'Bud Cauley', oddsB: -111 },
  { teamA: 'Angel Ayora', oddsA: -131, teamB: 'Jayden Schaper', oddsB: +101 },
  { teamA: 'J.T. Poston', oddsA: -102, teamB: 'Jordan Smith', oddsB: -128 },
  { teamA: 'Tom McKibbin', oddsA: -145, teamB: 'Haotong Li', oddsB: +115 },
  { teamA: 'Denny McCarthy', oddsA: +108, teamB: 'Taylor Pendrith', oddsB: -138 },
  { teamA: 'Matt Wallace', oddsA: -143, teamB: 'Daniel Hillier', oddsB: +113 },
  { teamA: 'Rasmus Neergaard Petersen', oddsA: -105, teamB: 'Christiaan Bezuidenhout', oddsB: -125 },
  { teamA: 'Ryo Hisatsune', oddsA: -136, teamB: 'Michael Kim', oddsB: +106 },
  { teamA: 'Andrew Novak', oddsA: -170, teamB: 'Bernd Wiesberger', oddsB: +135 },
  { teamA: 'Lucas Glover', oddsA: -125, teamB: 'Billy Horschel', oddsB: -105 },
  { teamA: 'Max Greyserman', oddsA: -120, teamB: 'Patrick Rodgers', oddsB: -110 },
  { teamA: 'Austin Smotherman', oddsA: -114, teamB: 'Rico Hoey', oddsB: -116 },
  { teamA: 'John Parry', oddsA: -105, teamB: 'Andrew Putnam', oddsB: -125 },
  { teamA: 'Mikael Lindberg', oddsA: -127, teamB: 'Casey Jarvis', oddsB: -103 },
  { teamA: 'Nico Echavarria', oddsA: -108, teamB: 'Sami Valimaki', oddsB: -122 },
  { teamA: 'Matti Schmid', oddsA: -172, teamB: 'Elvis Smylie', oddsB: +137 },
  { teamA: 'Ricky Castillo', oddsA: -149, teamB: 'Stewart Cink', oddsB: +119 },
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
