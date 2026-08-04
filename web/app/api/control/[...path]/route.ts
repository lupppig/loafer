const loaferdUrl = process.env.LOAFERD_URL ?? 'https://localhost:9443'
const trustedOrigins = new Set(
  (process.env.BETTER_AUTH_TRUSTED_ORIGINS ??
    process.env.BETTER_AUTH_URL ??
    'https://localhost:3000')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean),
)

if (!loaferdUrl.startsWith('https://')) {
  throw new Error('LOAFERD_URL must use HTTPS')
}

async function proxy(request: Request, context: { params: Promise<{ path: string[] }> }) {
  if (!['GET', 'HEAD', 'OPTIONS'].includes(request.method)) {
    const origin = request.headers.get('origin')
    if (!origin || !trustedOrigins.has(origin)) {
      return Response.json(
        { title: 'Origin rejected', status: 403, detail: 'Mutation origin is not trusted.' },
        { status: 403 },
      )
    }
  }
  const { auth } = await import('@/src/lib/auth')
  const session = await auth.api.getSession({ headers: request.headers })
  if (!session) {
    return Response.json(
      { title: 'Authentication required', status: 401, detail: 'Sign in to continue.' },
      { status: 401 },
    )
  }

  const token = (await auth.api.getToken({ headers: request.headers }))?.token
  if (!token) {
    return Response.json(
      {
        title: 'Authentication required',
        status: 401,
        detail: 'The session does not include a control-plane access token.',
      },
      { status: 401 },
    )
  }

  const { path } = await context.params
  if (path.some((segment) => !segment || segment === '.' || segment === '..' || /[\\/]/.test(segment))) {
    return Response.json(
      { title: 'Invalid path', status: 400, detail: 'Control-plane path is invalid.' },
      { status: 400 },
    )
  }
  const incoming = new URL(request.url)
  const target = new URL(`/api/v1/${path.join('/')}${incoming.search}`, loaferdUrl)
  const headers = new Headers()
  headers.set('Authorization', `Bearer ${token}`)
  headers.set('Accept', request.headers.get('accept') ?? 'application/json')
  headers.set('X-Request-ID', request.headers.get('x-request-id') ?? crypto.randomUUID())
  for (const name of ['content-type', 'idempotency-key', 'last-event-id']) {
    const value = request.headers.get(name)
    if (value) headers.set(name, value)
  }

  const response = await fetch(target, {
    method: request.method,
    headers,
    body: request.method === 'GET' || request.method === 'HEAD' ? undefined : await request.arrayBuffer(),
    cache: 'no-store',
    redirect: 'error',
    signal: path.at(-1) === 'stream' ? undefined : AbortSignal.timeout(30_000),
  })
  const outgoing = new Headers(response.headers)
  outgoing.delete('set-cookie')
  outgoing.delete('content-encoding')
  outgoing.delete('content-length')
  outgoing.set('Cache-Control', 'no-store')
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: outgoing,
  })
}

export const GET = proxy
export const POST = proxy
export const PUT = proxy
export const DELETE = proxy
