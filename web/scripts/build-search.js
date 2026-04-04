import fs from 'fs';
import path from 'path';

const DOCS_DIR = path.resolve('src/content/docs');
const OUT_FILE = path.resolve('public/search-index.json');
const EXCERPT_LENGTH = 100;

function parseFile(filePath, fileName) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const route = `/docs/${fileName.replace('.mdx', '')}`;
  
  const lines = content.split('\n');
  const items = [];
  let currentSection = '';
  let currentTitle = '';
  
  for(const line of lines) {
    if (line.startsWith('# ')) {
      currentTitle = line.replace('# ', '').trim();
      items.push({
        title: currentTitle,
        url: route,
        section: currentTitle,
        excerpt: ''
      });
    } else if (line.startsWith('## ') || line.startsWith('### ')) {
      const heading = line.replace(/#+ /, '').trim();
      currentSection = heading;
      const hash = heading.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
      items.push({
        title: heading,
        url: `${route}#${hash}`,
        section: currentTitle,
        excerpt: ''
      });
    } else if (line.trim().length > 10 && !line.startsWith('import ') && !line.startsWith('<') && !line.startsWith('`')) {
      if (items.length > 0) {
        const last = items[items.length - 1];
        if (last.excerpt.length < EXCERPT_LENGTH) {
          last.excerpt += line.trim() + ' ';
        }
      }
    }
  }
  
  return items.map(item => ({
    ...item,
    excerpt: item.excerpt.substring(0, EXCERPT_LENGTH).trim() + (item.excerpt.length > EXCERPT_LENGTH ? '...' : '')
  }));
}

function run() {
  if (!fs.existsSync(DOCS_DIR)) return;
  const files = fs.readdirSync(DOCS_DIR).filter(f => f.endsWith('.mdx'));
  const allItems = [];
  
  for (const file of files) {
    const items = parseFile(path.join(DOCS_DIR, file), file);
    allItems.push(...items);
  }
  
  if (!fs.existsSync(path.resolve('public'))) {
    fs.mkdirSync(path.resolve('public'), { recursive: true });
  }
  fs.writeFileSync(OUT_FILE, JSON.stringify(allItems, null, 2));
  console.log(`Generated search-index.json with ${allItems.length} entries.`);
}

run();
