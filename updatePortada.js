const fs = require('fs');
const path = require('path');

const filePath = 'c:\\Users\\lgalv\\Desktop\\obsidian\\portada.html';
let html = fs.readFileSync(filePath, 'utf8');

// Replace the giant base64 img tag inside .art
const regex = /<div class="art"><img[\s\S]*?<\/div>/;
const newArt = '<div class="art" style="top: 3.5in; left: 0.6in; right: 0.6in;"><img src="condor_profesional.jpg" alt="Cóndor Andino" style="width: 100%; max-height: 4.6in; object-fit: contain; display: block; margin: 0 auto; filter: contrast(1.05);"></div>';

if (regex.test(html)) {
  html = html.replace(regex, newArt);
  fs.writeFileSync(filePath, html, 'utf8');
  console.log('portada.html successfully updated with professional condor illustration!');
} else {
  console.log('Warning: regex did not match .art block in portada.html');
}
