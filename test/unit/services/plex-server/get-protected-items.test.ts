import type { User } from '@root/types/config.types.js'
import { PlexServerService } from '@services/plex-server.service.js'
import { HttpResponse, http } from 'msw'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createMockLogger } from '../../../mocks/logger.js'
import { server } from '../../../setup/msw-setup.js'

function makeUser(id: number, plexUuid: string | null): User {
  return {
    id,
    name: `user-${id}`,
    apprise: null,
    alias: null,
    discord_id: null,
    notify_apprise: false,
    notify_discord: false,
    notify_discord_mention: false,
    notify_plex_mobile: false,
    can_sync: true,
    requires_approval: false,
    is_primary_token: id === 1,
    plex_uuid: plexUuid,
    avatar: null,
    display_name: null,
    friend_created_at: null,
    created_at: '2024-01-01T00:00:00.000Z',
    updated_at: '2024-01-01T00:00:00.000Z',
  }
}

function makeCustomListsResponse(
  items: Array<{ id: string; title: string; type: string }>,
) {
  const nodes =
    items.length > 0
      ? [
          {
            id: 'list-uuid-1',
            name: 'Do Not Delete',
            metadataItems: {
              nodes: items,
              pageInfo: { hasNextPage: false, endCursor: null },
            },
          },
        ]
      : []
  return {
    data: {
      userV2: {
        customLists: {
          nodes,
          pageInfo: { hasNextPage: false, endCursor: null },
        },
      },
    },
  }
}

describe('PlexServerService.getProtectedItems (Lists-based)', () => {
  let mockLogger: ReturnType<typeof createMockLogger>
  let serviceLogger: ReturnType<typeof createMockLogger>
  let mockDb: { getAllUsers: ReturnType<typeof vi.fn> }
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let mockFastify: any
  let service: PlexServerService

  beforeEach(() => {
    vi.clearAllMocks()
    mockLogger = createMockLogger()
    serviceLogger = createMockLogger()
    ;(mockLogger.child as ReturnType<typeof vi.fn>).mockReturnValue(
      serviceLogger,
    )
    mockDb = { getAllUsers: vi.fn() }
    mockFastify = {
      config: {
        enablePlexListProtection: true,
        plexTokens: ['admin-token'],
        plexProtectionListName: 'Do Not Delete',
      },
      db: mockDb,
    }
    service = new PlexServerService(mockLogger, mockFastify)
  })

  it('returns empty set immediately when playlist protection is disabled', async () => {
    mockFastify.config.enablePlexListProtection = false

    const result = await service.getProtectedItems()

    expect(result).toBeInstanceOf(Set)
    expect(result.size).toBe(0)
    expect(mockDb.getAllUsers).not.toHaveBeenCalled()
  })

  it('returns empty set when no users have a plex_uuid', async () => {
    mockDb.getAllUsers.mockResolvedValue([makeUser(1, null), makeUser(2, null)])

    const result = await service.getProtectedItems()

    expect(result.size).toBe(0)
  })

  it('logs a warning when no users have a plex_uuid', async () => {
    mockDb.getAllUsers.mockResolvedValue([makeUser(1, null), makeUser(2, null)])

    await service.getProtectedItems()

    expect(serviceLogger.warn).toHaveBeenCalledWith(
      expect.stringContaining('plex_uuid'),
    )
  })

  it('returns empty set when no users have a matching protection list', async () => {
    mockDb.getAllUsers.mockResolvedValue([makeUser(1, 'uuid-1')])
    server.use(
      http.post('https://community.plex.tv/api', () =>
        HttpResponse.json(makeCustomListsResponse([])),
      ),
    )

    const result = await service.getProtectedItems()

    expect(result.size).toBe(0)
  })

  it('resolves and returns external GUIDs for items in the protection list', async () => {
    mockDb.getAllUsers.mockResolvedValue([makeUser(1, 'uuid-1')])
    server.use(
      http.post('https://community.plex.tv/api', () =>
        HttpResponse.json(
          makeCustomListsResponse([
            { id: 'plex://movie/abc', title: 'The Matrix', type: 'movie' },
            { id: 'plex://show/def', title: 'Breaking Bad', type: 'show' },
          ]),
        ),
      ),
    )
    vi.spyOn(service, 'getItemMetadata')
      .mockResolvedValueOnce({ title: 'The Matrix', guids: ['tmdb://603'] })
      .mockResolvedValueOnce({ title: 'Breaking Bad', guids: ['tvdb://81189'] })

    const result = await service.getProtectedItems()

    expect(result.size).toBe(2)
    expect(result.has('tmdb://603')).toBe(true)
    expect(result.has('tvdb://81189')).toBe(true)
  })

  it('aggregates GUIDs from multiple users with protection lists', async () => {
    mockDb.getAllUsers.mockResolvedValue([
      makeUser(1, 'uuid-1'),
      makeUser(2, 'uuid-2'),
    ])
    server.use(
      http.post('https://community.plex.tv/api', () =>
        HttpResponse.json(
          makeCustomListsResponse([
            { id: 'plex://movie/abc', title: 'The Matrix', type: 'movie' },
          ]),
        ),
      ),
    )
    vi.spyOn(service, 'getItemMetadata')
      .mockResolvedValueOnce({ title: 'The Matrix', guids: ['tmdb://603'] })
      .mockResolvedValueOnce({
        title: 'The Matrix',
        guids: ['tmdb://603', 'imdb://tt0133093'],
      })

    const result = await service.getProtectedItems()

    expect(result.has('tmdb://603')).toBe(true)
    expect(result.has('imdb://tt0133093')).toBe(true)
  })

  it('skips users with null plex_uuid and only queries users that have one', async () => {
    mockDb.getAllUsers.mockResolvedValue([
      makeUser(1, 'uuid-1'),
      makeUser(2, null),
    ])
    server.use(
      http.post('https://community.plex.tv/api', () =>
        HttpResponse.json(
          makeCustomListsResponse([
            { id: 'plex://movie/abc', title: 'The Matrix', type: 'movie' },
          ]),
        ),
      ),
    )
    const spy = vi
      .spyOn(service, 'getItemMetadata')
      .mockResolvedValue({ title: 'The Matrix', guids: ['tmdb://603'] })

    await service.getProtectedItems()

    expect(spy).toHaveBeenCalledTimes(1)
  })

  it('returns cached result on second call without re-fetching', async () => {
    mockDb.getAllUsers.mockResolvedValue([makeUser(1, 'uuid-1')])
    server.use(
      http.post('https://community.plex.tv/api', () =>
        HttpResponse.json(
          makeCustomListsResponse([
            { id: 'plex://movie/abc', title: 'The Matrix', type: 'movie' },
          ]),
        ),
      ),
    )
    vi.spyOn(service, 'getItemMetadata').mockResolvedValue({
      title: 'The Matrix',
      guids: ['tmdb://603'],
    })

    const first = await service.getProtectedItems()
    const second = await service.getProtectedItems()

    expect(second).toBe(first)
    expect(mockDb.getAllUsers).toHaveBeenCalledTimes(1)
  })

  it('continues and warns when getItemMetadata returns null for an item', async () => {
    mockDb.getAllUsers.mockResolvedValue([makeUser(1, 'uuid-1')])
    server.use(
      http.post('https://community.plex.tv/api', () =>
        HttpResponse.json(
          makeCustomListsResponse([
            { id: 'plex://movie/aaa', title: 'Orphaned Item', type: 'movie' },
            { id: 'plex://movie/bbb', title: 'The Matrix', type: 'movie' },
          ]),
        ),
      ),
    )
    vi.spyOn(service, 'getItemMetadata')
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce({ title: 'The Matrix', guids: ['tmdb://603'] })

    const result = await service.getProtectedItems()

    expect(result.size).toBe(1)
    expect(result.has('tmdb://603')).toBe(true)
  })

  it('throws on API error to prevent delete-sync from proceeding unprotected', async () => {
    mockDb.getAllUsers.mockResolvedValue([makeUser(1, 'uuid-1')])
    server.use(
      http.post('https://community.plex.tv/api', () =>
        HttpResponse.json({
          errors: [{ message: 'Unauthorized' }],
        }),
      ),
    )

    await expect(service.getProtectedItems()).rejects.toThrow()
  })

  it('calls getItemMetadata with the Plex cloud item id and type from the list', async () => {
    mockDb.getAllUsers.mockResolvedValue([makeUser(1, 'uuid-1')])
    server.use(
      http.post('https://community.plex.tv/api', () =>
        HttpResponse.json(
          makeCustomListsResponse([
            { id: 'plex://movie/abc123', title: 'The Matrix', type: 'movie' },
          ]),
        ),
      ),
    )
    const spy = vi
      .spyOn(service, 'getItemMetadata')
      .mockResolvedValue({ title: 'The Matrix', guids: ['tmdb://603'] })

    await service.getProtectedItems()

    expect(spy).toHaveBeenCalledWith(
      'user-1',
      'plex://movie/abc123',
      undefined,
      'movie',
    )
  })
})
