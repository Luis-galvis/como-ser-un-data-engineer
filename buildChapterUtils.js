const fs = require('fs');
const path = require('path');

const VAULT_PATH = 'D:\\estudio\\estudio';
const CAP_DIR = 'c:\\Users\\lgalv\\Desktop\\obsidian\\libro\\capitulos';
const MIRROR_DIR = 'D:\\estudio\\libro\\capitulos';

if (!fs.existsSync(CAP_DIR)) fs.mkdirSync(CAP_DIR, { recursive: true });
if (fs.existsSync('D:\\estudio\\libro') && !fs.existsSync(MIRROR_DIR)) fs.mkdirSync(MIRROR_DIR, { recursive: true });

function readNote(noteName) {
  const p = path.join(VAULT_PATH, noteName.endsWith('.md') ? noteName : noteName + '.md');
  if (!fs.existsSync(p)) return null;
  return fs.readFileSync(p, 'utf8');
}

function cleanWikilinks(text) {
  // Convert [[Note|Alias]] -> Alias, [[Note]] -> Note
  return text.replace(/\[\[([^|\]\n]+)\|([^\]\n]+)\]\]/g, '$2')
             .replace(/\[\[([^\]\n]+)\]\]/g, '$1');
}

console.log('Helper functions loaded.');
