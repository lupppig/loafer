async function handler(request: Request) {
  // Runtime-only initialization keeps database credentials out of the build step.
  const { auth } = await import('@/src/lib/auth')
  return auth.handler(request)
}

export const GET = handler
export const POST = handler
