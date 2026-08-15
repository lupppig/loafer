# Open Graph fonts

Geist, for the social card rendered by `app/opengraph-image.tsx`.

These are TTF, not the woff2 the site itself uses, and they have had their
layout tables removed. Both facts are forced by satori, the renderer behind
`next/og`:

- it cannot parse woff2 at all;
- it rejects Geist's `GSUB` table with `lookupType: 6 - substFormat: 1 is not
  yet supported`.

The card sets plain Latin text with no ligature or contextual substitution, so
dropping `GSUB`, `GPOS`, and `GDEF` costs nothing.

## Regenerating

From the woff builds shipped by `@fontsource`:

```bash
pip install fonttools
cd web/app/_og

python - <<'PY'
from fontTools.ttLib import TTFont

for name in ('geist-sans-700', 'geist-sans-400', 'geist-mono-500'):
    font = TTFont(f'{name}.woff')
    for table in ('GSUB', 'GPOS', 'GDEF'):
        if table in font:
            del font[table]
    font.flavor = None
    font.save(f'{name}.ttf')
PY
```

The source woff files are at
`web/node_modules/@fontsource/geist-{sans,mono}/files/`.

`next.config.mjs` keeps this directory in the standalone output through
`outputFileTracingIncludes`, because the route reads the files from disk rather
than importing them.
