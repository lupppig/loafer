'use client'

import { apiKeyClient } from '@better-auth/api-key/client'
import { createAuthClient } from 'better-auth/react'
import {
  adminClient,
  deviceAuthorizationClient,
  organizationClient,
} from 'better-auth/client/plugins'

export const authClient = createAuthClient({
  plugins: [
    adminClient(),
    organizationClient(),
    deviceAuthorizationClient(),
    apiKeyClient(),
  ],
})
