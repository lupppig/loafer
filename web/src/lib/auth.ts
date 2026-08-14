import { apiKey } from '@better-auth/api-key'
import { betterAuth } from 'better-auth'
import { nextCookies } from 'better-auth/next-js'
import {
  admin,
  bearer,
  deviceAuthorization,
  jwt,
  organization,
} from 'better-auth/plugins'
import { DatabaseSync } from 'node:sqlite'
import { Pool } from 'pg'

const authBaseUrl = process.env.BETTER_AUTH_URL ?? 'https://localhost:3000'
const controlPlaneAudience = process.env.LOAFERD_AUDIENCE ?? 'https://localhost:9443'
const trustedOrigins = (process.env.BETTER_AUTH_TRUSTED_ORIGINS ?? authBaseUrl)
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean)

for (const [name, value] of [
  ['BETTER_AUTH_URL', authBaseUrl],
  ['LOAFERD_AUDIENCE', controlPlaneAudience],
  ...trustedOrigins.map((origin) => ['BETTER_AUTH_TRUSTED_ORIGINS', origin]),
]) {
  if (!value.startsWith('https://')) {
    throw new Error(`${name} must use HTTPS`)
  }
}

const databaseUrl = process.env.BETTER_AUTH_DATABASE_URL
const authEmailEndpoint = process.env.LOAFER_AUTH_EMAIL_ENDPOINT
// The SQLite fallback is a local-development convenience. In a container it
// silently tries to create a file on a read-only root and surfaces as
// "unable to open database file" on the first request, so refuse it early with
// a message that names the variable to set.
if (!databaseUrl && process.env.NODE_ENV === 'production') {
  throw new Error(
    'BETTER_AUTH_DATABASE_URL is required in production. The embedded SQLite ' +
      'fallback is for local development only.',
  )
}

const database = databaseUrl
  ? new Pool({ connectionString: databaseUrl })
  : new DatabaseSync(process.env.BETTER_AUTH_SQLITE_PATH ?? '.loafer-auth.db')

export const auth = betterAuth({
  appName: 'Loafer',
  baseURL: authBaseUrl,
  database,
  trustedOrigins,
  emailAndPassword: {
    enabled: true,
    disableSignUp: process.env.LOAFER_ALLOW_SIGNUP !== '1',
    requireEmailVerification: true,
    autoSignIn: false,
    minPasswordLength: 12,
    maxPasswordLength: 128,
    revokeSessionsOnPasswordReset: true,
    sendResetPassword: async ({ user, url }) => {
      await sendAuthEmail('password-reset', user.email, url)
    },
  },
  emailVerification: {
    sendVerificationEmail: async ({ user, url }) => {
      await sendAuthEmail('email-verification', user.email, url)
    },
  },
  session: {
    expiresIn: 60 * 60 * 24 * 7,
    updateAge: 60 * 60 * 12,
  },
  rateLimit: {
    enabled: true,
    window: 60,
    max: 100,
    storage: 'database',
    customRules: {
      '/sign-in/email': { window: 60, max: 10 },
      '/sign-up/email': { window: 60, max: 5 },
      '/device/code': { window: 60, max: 10 },
      '/device/token': { window: 60, max: 60 },
    },
  },
  advanced: {
    useSecureCookies: true,
    cookiePrefix: 'loafer',
    defaultCookieAttributes: {
      httpOnly: true,
      secure: true,
      sameSite: 'lax',
    },
  },
  plugins: [
    admin(),
    organization({
      sendInvitationEmail: async ({ email, invitation }) => {
        const invitationUrl = new URL('/accept-invitation', authBaseUrl)
        invitationUrl.searchParams.set('id', invitation.id)
        await sendAuthEmail('organization-invitation', email, invitationUrl.toString())
      },
    }),
    deviceAuthorization({
      verificationUri: '/device',
      validateClient: (clientId) => clientId === 'loafer-cli',
    }),
    apiKey({
      configId: 'automation',
      references: 'organization',
      defaultPrefix: 'loafer_',
      requireName: true,
      enableMetadata: false,
      enableSessionForAPIKeys: true,
      keyExpiration: {
        defaultExpiresIn: 1000 * 60 * 60 * 24 * 30,
        minExpiresIn: 1,
        maxExpiresIn: 90,
      },
      rateLimit: {
        enabled: true,
        timeWindow: 60_000,
        maxRequests: 120,
      },
      permissions: {
        defaultPermissions: {
          controlPlane: ['read'],
        },
      },
    }),
    bearer(),
    jwt({
      jwks: {
        keyPairConfig: { alg: 'EdDSA', crv: 'Ed25519' },
        rotationInterval: 60 * 60 * 24 * 30,
        gracePeriod: 60 * 60 * 24 * 30,
      },
      jwt: {
        issuer: authBaseUrl,
        audience: controlPlaneAudience,
        expirationTime: '15m',
        definePayload: ({ user }) => ({
          email: user.email,
          role: user.role,
        }),
      },
    }),
    nextCookies(),
  ],
})

async function sendAuthEmail(kind: string, email: string, actionUrl: string) {
  if (!authEmailEndpoint?.startsWith('https://')) {
    throw new Error('LOAFER_AUTH_EMAIL_ENDPOINT must be configured with an HTTPS URL')
  }
  const response = await fetch(authEmailEndpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ kind, email, action_url: actionUrl }),
  })
  if (!response.ok) {
    throw new Error(`authentication email delivery failed with status ${response.status}`)
  }
}
