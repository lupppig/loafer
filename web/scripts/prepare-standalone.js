import fs from 'node:fs'
import path from 'node:path'

const projectRoot = process.cwd()
const standaloneRoot = path.join(projectRoot, '.next', 'standalone')

if (!fs.existsSync(standaloneRoot)) {
  throw new Error('Next.js standalone output was not generated')
}

const copies = [
  [path.join(projectRoot, 'public'), path.join(standaloneRoot, 'public')],
  [
    path.join(projectRoot, '.next', 'static'),
    path.join(standaloneRoot, '.next', 'static'),
  ],
]

for (const [source, destination] of copies) {
  fs.rmSync(destination, { force: true, recursive: true })
  fs.cpSync(source, destination, { recursive: true })
}

console.log('Prepared self-contained Next.js standalone output.')
