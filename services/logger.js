const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };

let currentLevel = LEVELS.info;

function setLevel(level) {
  currentLevel = LEVELS[level] || LEVELS.info;
}

function timestamp() {
  return new Date().toISOString();
}

function log(level, category, message, data) {
  if (LEVELS[level] < currentLevel) return;
  const prefix = `${timestamp()} [${category}]`;
  const line = data !== undefined
    ? `${prefix} ${message} ${typeof data === 'object' ? JSON.stringify(data) : data}`
    : `${prefix} ${message}`;

  if (level === 'error') {
    console.error(line);
  } else if (level === 'warn') {
    console.warn(line);
  } else {
    console.log(line);
  }
}

module.exports = {
  setLevel,
  debug: (cat, msg, data) => log('debug', cat, msg, data),
  info:  (cat, msg, data) => log('info', cat, msg, data),
  warn:  (cat, msg, data) => log('warn', cat, msg, data),
  error: (cat, msg, data) => log('error', cat, msg, data),
};
