import type { Friend } from '@root/types/plex.types.js'
import { getCustomListsForUser } from '@services/plex-watchlist/index.js'
import { HttpResponse, http } from 'msw'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createMockLogger } from '../../../mocks/logger.js'
import { server } from '../../../setup/msw-setup.js'

describe('plex/custom-lists-api', () => {
  const mockLogger = createMockLogger()

  const user: Friend = {
    watchlistId: 'user-cloud-123',
    username: 'testuser',
    userId: 1,
  }

  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    server.resetHandlers()
  })

  // Phase 1 response: list names only, no metadataItems
  function phase1Response(
    name: string,
    id: string,
    hasNextPage = false,
    endCursor: string | null = null,
  ) {
    return {
      data: {
        userV2: {
          customLists: {
            nodes: [{ id, name }],
            pageInfo: { hasNextPage, endCursor },
          },
        },
      },
    }
  }

  // Phase 2 response: items for a single list (must include name for node lookup)
  function phase2Response(
    items: Array<{ id: string; title: string; type: string }>,
    hasNextPage = false,
    endCursor: string | null = null,
    listName = 'Do Not Delete',
  ) {
    return {
      data: {
        userV2: {
          customLists: {
            nodes: [
              {
                id: 'list-uuid-1',
                name: listName,
                metadataItems: {
                  nodes: items,
                  pageInfo: { hasNextPage, endCursor },
                },
              },
            ],
          },
        },
      },
    }
  }

  it('returns metadata items from the list matching the protection name', async () => {
    server.use(
      http.post('https://community.plex.tv/api', async ({ request }) => {
        const body = (await request.json()) as { query: string }
        if (body.query.includes('metadataItems')) {
          return HttpResponse.json(
            phase2Response([
              { id: 'plex://movie/abc123', title: 'The Matrix', type: 'movie' },
              { id: 'plex://show/def456', title: 'Breaking Bad', type: 'show' },
            ]),
          )
        }
        return HttpResponse.json(phase1Response('Do Not Delete', 'list-uuid-1'))
      }),
    )

    const result = await getCustomListsForUser(
      mockLogger,
      'admin-token',
      user,
      'Do Not Delete',
    )

    expect(result).toHaveLength(2)
    expect(result[0]).toEqual({
      id: 'plex://movie/abc123',
      title: 'The Matrix',
      type: 'movie',
    })
    expect(result[1]).toEqual({
      id: 'plex://show/def456',
      title: 'Breaking Bad',
      type: 'show',
    })
  })

  it('returns empty array when no list matches the protection name', async () => {
    server.use(
      http.post('https://community.plex.tv/api', () =>
        HttpResponse.json(phase1Response('Favourites', 'list-uuid-1')),
      ),
    )

    const result = await getCustomListsForUser(
      mockLogger,
      'admin-token',
      user,
      'Do Not Delete',
    )

    expect(result).toHaveLength(0)
  })

  it('returns empty array when user has no custom lists', async () => {
    server.use(
      http.post('https://community.plex.tv/api', () =>
        HttpResponse.json({
          data: {
            userV2: {
              customLists: {
                nodes: [],
                pageInfo: { hasNextPage: false, endCursor: null },
              },
            },
          },
        }),
      ),
    )

    const result = await getCustomListsForUser(
      mockLogger,
      'admin-token',
      user,
      'Do Not Delete',
    )

    expect(result).toHaveLength(0)
  })

  it('Phase 1 discovery query includes customLists but not metadataItems', async () => {
    let phase1Query: string | null = null

    server.use(
      http.post('https://community.plex.tv/api', async ({ request }) => {
        const body = (await request.json()) as { query: string }
        if (!body.query.includes('metadataItems')) {
          phase1Query = body.query
          return HttpResponse.json({
            data: {
              userV2: {
                customLists: {
                  nodes: [],
                  pageInfo: { hasNextPage: false, endCursor: null },
                },
              },
            },
          })
        }
        return HttpResponse.json(phase2Response([]))
      }),
    )

    await getCustomListsForUser(mockLogger, 'admin-token', user, 'Do Not Delete')

    expect(phase1Query).not.toBeNull()
    expect(phase1Query).toContain('customLists')
    expect(phase1Query).not.toContain('metadataItems')
  })

  it('sends user watchlistId in query variables', async () => {
    let capturedBody: unknown = null

    server.use(
      http.post('https://community.plex.tv/api', async ({ request }) => {
        capturedBody = await request.json()
        return HttpResponse.json({
          data: {
            userV2: {
              customLists: {
                nodes: [],
                pageInfo: { hasNextPage: false, endCursor: null },
              },
            },
          },
        })
      }),
    )

    await getCustomListsForUser(mockLogger, 'admin-token', user, 'Do Not Delete')

    expect(capturedBody).toHaveProperty('variables')
    expect(
      (capturedBody as { variables: { user: { id: string } } }).variables.user
        .id,
    ).toBe('user-cloud-123')
  })

  it('sends X-Plex-Token header with the provided admin token', async () => {
    let capturedHeaders: Headers | undefined

    server.use(
      http.post('https://community.plex.tv/api', ({ request }) => {
        capturedHeaders = request.headers
        return HttpResponse.json({
          data: {
            userV2: {
              customLists: {
                nodes: [],
                pageInfo: { hasNextPage: false, endCursor: null },
              },
            },
          },
        })
      }),
    )

    await getCustomListsForUser(
      mockLogger,
      'my-admin-token',
      user,
      'Do Not Delete',
    )

    expect(capturedHeaders?.get('X-Plex-Token')).toBe('my-admin-token')
  })

  it('pages through customLists to find a matching list on a later page', async () => {
    let phase1CallCount = 0

    server.use(
      http.post('https://community.plex.tv/api', async ({ request }) => {
        const body = (await request.json()) as { query: string }
        if (body.query.includes('metadataItems')) {
          return HttpResponse.json(
            phase2Response([
              { id: 'plex://movie/abc123', title: 'The Matrix', type: 'movie' },
            ]),
          )
        }
        phase1CallCount++
        if (phase1CallCount === 1) {
          return HttpResponse.json(
            phase1Response('Favourites', 'list-uuid-1', true, 'cursor-1'),
          )
        }
        return HttpResponse.json(phase1Response('Do Not Delete', 'list-uuid-2'))
      }),
    )

    const result = await getCustomListsForUser(
      mockLogger,
      'admin-token',
      user,
      'Do Not Delete',
    )

    expect(result).toHaveLength(1)
    expect(result[0].title).toBe('The Matrix')
    expect(phase1CallCount).toBe(2)
  })

  it('stops paging once the matching list is found on the first page', async () => {
    let callCount = 0

    server.use(
      http.post('https://community.plex.tv/api', async ({ request }) => {
        callCount++
        const body = (await request.json()) as { query: string }
        if (body.query.includes('metadataItems')) {
          return HttpResponse.json(
            phase2Response([
              { id: 'plex://movie/abc123', title: 'The Matrix', type: 'movie' },
            ]),
          )
        }
        // Phase 1 - match found immediately; more pages exist but must not be followed
        return HttpResponse.json(
          phase1Response('Do Not Delete', 'list-uuid-1', true, 'cursor-1'),
        )
      }),
    )

    const result = await getCustomListsForUser(
      mockLogger,
      'admin-token',
      user,
      'Do Not Delete',
    )

    expect(result).toHaveLength(1)
    expect(callCount).toBe(2) // Phase 1 (found) + Phase 2 (items)
  })

  it('paginates metadataItems when a list has more than one page of items', async () => {
    let phase2CallCount = 0

    server.use(
      http.post('https://community.plex.tv/api', async ({ request }) => {
        const body = (await request.json()) as { query: string }
        if (body.query.includes('metadataItems')) {
          phase2CallCount++
          if (phase2CallCount === 1) {
            return HttpResponse.json(
              phase2Response(
                [
                  {
                    id: 'plex://movie/abc123',
                    title: 'The Matrix',
                    type: 'movie',
                  },
                ],
                true,
                'item-cursor-1',
              ),
            )
          }
          return HttpResponse.json(
            phase2Response([
              {
                id: 'plex://show/def456',
                title: 'Breaking Bad',
                type: 'show',
              },
            ]),
          )
        }
        return HttpResponse.json(phase1Response('Do Not Delete', 'list-uuid-1'))
      }),
    )

    const result = await getCustomListsForUser(
      mockLogger,
      'admin-token',
      user,
      'Do Not Delete',
    )

    expect(result).toHaveLength(2)
    expect(result[0].title).toBe('The Matrix')
    expect(result[1].title).toBe('Breaking Bad')
    expect(phase2CallCount).toBe(2)
  })

  it('matches list name case-insensitively', async () => {
    server.use(
      http.post('https://community.plex.tv/api', async ({ request }) => {
        const body = (await request.json()) as { query: string }
        if (body.query.includes('metadataItems')) {
          return HttpResponse.json(
            phase2Response(
              [{ id: 'plex://movie/abc123', title: 'The Matrix', type: 'movie' }],
              false,
              null,
              'do not delete',
            ),
          )
        }
        // lowercase list name — must still match 'Do Not Delete'
        return HttpResponse.json(phase1Response('do not delete', 'list-uuid-1'))
      }),
    )

    const result = await getCustomListsForUser(
      mockLogger,
      'admin-token',
      user,
      'Do Not Delete',
    )

    expect(result).toHaveLength(1)
  })

  it('throws when user has no watchlistId', async () => {
    const invalidUser = { username: 'testuser', userId: 1 } as unknown as Friend

    await expect(
      getCustomListsForUser(
        mockLogger,
        'admin-token',
        invalidUser,
        'Do Not Delete',
      ),
    ).rejects.toThrow('Invalid user object provided to getCustomListsForUser')
  })

  it('throws on 429 to prevent proceeding with unprotected deletion', async () => {
    server.use(
      http.post('https://community.plex.tv/api', () =>
        new HttpResponse(null, { status: 429 }),
      ),
    )

    await expect(
      getCustomListsForUser(mockLogger, 'admin-token', user, 'Do Not Delete'),
    ).rejects.toThrow()
  })

  it('throws on GraphQL errors to prevent proceeding unprotected', async () => {
    server.use(
      http.post('https://community.plex.tv/api', () =>
        HttpResponse.json({
          errors: [{ message: 'Unauthorized' }],
        }),
      ),
    )

    await expect(
      getCustomListsForUser(mockLogger, 'admin-token', user, 'Do Not Delete'),
    ).rejects.toThrow('GraphQL errors')
  })

  it('throws on network error to prevent proceeding unprotected', async () => {
    server.use(
      http.post('https://community.plex.tv/api', () => HttpResponse.error()),
    )

    await expect(
      getCustomListsForUser(mockLogger, 'admin-token', user, 'Do Not Delete'),
    ).rejects.toThrow()
  })
})
