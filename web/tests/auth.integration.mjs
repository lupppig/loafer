import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { DatabaseSync } from 'node:sqlite'
import { unlinkSync } from 'node:fs'
import { after, test } from 'node:test'
import { pathToFileURL } from 'node:url'
import { getMigrations } from 'better-auth/db/migration'

const originalFetch = globalThis.fetch
const databasePaths = []

process.env.BETTER_AUTH_SECRET = 'loafer-test-secret-0123456789-0123456789'
process.env.BETTER_AUTH_URL = 'https://auth.test'
process.env.BETTER_AUTH_TRUSTED_ORIGINS = 'https://app.test'
process.env.LOAFERD_AUDIENCE = 'https://api.test'
process.env.LOAFER_AUTH_EMAIL_ENDPOINT = 'https://email.test/send'
globalThis.fetch = async (input, init) => {
  if (String(input) === process.env.LOAFER_AUTH_EMAIL_ENDPOINT) {
    assert.equal(init?.method, 'POST')
    return new Response(null, { status: 204 })
  }
  return originalFetch(input, init)
}

after(() => {
  globalThis.fetch = originalFetch
  for (const path of databasePaths) {
    try {
      unlinkSync(path)
    } catch {
      // The operating system removes /tmp fixtures after the test environment exits.
    }
  }
})

async function loadAuth(profile, allowSignup) {
  const databasePath = `/tmp/loafer-auth-${profile}-${randomUUID()}.db`
  databasePaths.push(databasePath)
  process.env.BETTER_AUTH_SQLITE_PATH = databasePath
  if (allowSignup) process.env.LOAFER_ALLOW_SIGNUP = '1'
  else delete process.env.LOAFER_ALLOW_SIGNUP
  const moduleUrl = pathToFileURL(new URL('../src/lib/auth.ts', import.meta.url).pathname)
  moduleUrl.searchParams.set('profile', profile)
  const { auth } = await import(moduleUrl.href)
  const { runMigrations } = await getMigrations(auth.options)
  await runMigrations()
  return { auth, databasePath }
}

function authRequest(path, { body, cookie, origin = 'https://app.test' } = {}) {
  const headers = new Headers({ Origin: origin })
  if (body !== undefined) headers.set('Content-Type', 'application/json')
  if (cookie) headers.set('Cookie', cookie)
  return new Request(`https://auth.test/api/auth${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  })
}

function sessionCookie(response) {
  const setCookie = response.headers.get('set-cookie') ?? ''
  const match = setCookie.match(/(?:^|,\s*)([^=;]*loafer\.session_token)=([^;]+)/)
  assert.ok(match, `missing secure session cookie: ${setCookie}`)
  assert.match(setCookie, /HttpOnly/i)
  assert.match(setCookie, /Secure/i)
  assert.match(setCookie, /SameSite=Lax/i)
  return `${match[1]}=${match[2]}`
}

test('public signup is disabled in the bootstrap profile', async () => {
  const { auth } = await loadAuth('disabled', false)
  const response = await auth.handler(
    authRequest('/sign-up/email', {
      body: { name: 'Blocked', email: 'blocked@example.com', password: 'correct-horse-battery' },
    }),
  )
  assert.ok([400, 403].includes(response.status), await response.text())
})

test('session logout revokes the cookie and untrusted origins fail closed', async () => {
  const { auth, databasePath } = await loadAuth('session', true)
  const signup = await auth.handler(
    authRequest('/sign-up/email', {
      body: { name: 'Operator', email: 'operator@example.com', password: 'correct-horse-battery' },
    }),
  )
  assert.equal(signup.status, 200, await signup.text())
  const database = new DatabaseSync(databasePath)
  database.prepare('UPDATE user SET emailVerified = 1 WHERE email = ?').run('operator@example.com')
  database.close()

  const rejectedOrigin = await auth.handler(
    authRequest('/sign-in/email', {
      origin: 'https://evil.example',
      body: { email: 'operator@example.com', password: 'correct-horse-battery' },
    }),
  )
  assert.equal(rejectedOrigin.status, 403)

  const login = await auth.handler(
    authRequest('/sign-in/email', {
      body: { email: 'operator@example.com', password: 'correct-horse-battery' },
    }),
  )
  assert.equal(login.status, 200, await login.text())
  const cookie = sessionCookie(login)

  const authenticated = await auth.handler(authRequest('/get-session', { cookie }))
  assert.equal(authenticated.status, 200)
  assert.equal((await authenticated.json()).user.email, 'operator@example.com')

  const secondLogin = await auth.handler(
    authRequest('/sign-in/email', {
      body: { email: 'operator@example.com', password: 'correct-horse-battery' },
    }),
  )
  assert.equal(secondLogin.status, 200, await secondLogin.text())
  const rotatedCookie = sessionCookie(secondLogin)
  assert.notEqual(rotatedCookie, cookie)

  const logout = await auth.handler(
    authRequest('/sign-out', { body: {}, cookie: rotatedCookie }),
  )
  assert.equal(logout.status, 200, await logout.text())
  const revoked = await auth.handler(authRequest('/get-session', { cookie: rotatedCookie }))
  assert.equal(await revoked.text(), 'null')

  const attempts = []
  for (let index = 0; index < 12; index += 1) {
    attempts.push(
      auth.handler(
        authRequest('/sign-in/email', {
          body: { email: 'operator@example.com', password: 'definitely-wrong-password' },
        }),
      ),
    )
  }
  const statuses = (await Promise.all(attempts)).map((response) => response.status)
  assert.ok(statuses.includes(429), `expected rate limit response, received ${statuses}`)
})
