/**
 * Plex Server Service
 *
 * A stateful service class for interacting with Plex Media Server.
 * Provides connection management, user operations, and playlist protection functionality.
 */

import type { Item } from '@root/types/plex.types.js'
import type {
  PlexMetadata,
  PlexResource,
  PlexServerConnectionInfo,
  PlexUser,
} from '@root/types/plex-server.types.js'
import type {
  PlexChildrenResponse,
  PlexSession,
  PlexShowMetadata,
  PlexShowMetadataResponse,
} from '@root/types/plex-session.types.js'
import {
  getCustomListsForUser,
  toItemsSingle,
} from '@services/plex-watchlist/index.js'
import { buildPlexGuid, parseGuids } from '@utils/guid-handler.js'
import { createServiceLogger } from '@utils/logger.js'
import { isSameServerEndpoint } from '@utils/url.js'
import { PLEX_CLIENT_IDENTIFIER, USER_AGENT } from '@utils/version.js'
import { XMLParser } from 'fast-xml-parser'
import type { FastifyBaseLogger, FastifyInstance } from 'fastify'
import {
  buildUniqueServerList,
  type CachedConnection,
  type CachedContentAvailability,
  type ConnectionCandidate,
  checkContentOnServer,
  clearContentCacheForReconciliation,
  getBestServerConnection,
  testConnectionReachability,
} from './plex-server/existence-check/index.js'
import {
  getCurrentLabels,
  getMetadata,
  removeSpecificLabels,
  updateLabels,
} from './plex-server/labels/index.js'
import {
  getMetadataChildren,
  getShowMetadata,
  searchByGuid,
} from './plex-server/metadata/index.js'
import { getAllPlexResources } from './plex-server/resources/resource-operations.js'
import { getActiveSessions } from './plex-server/sessions/session-operations.js'
import {
  PlexEventSource,
  type PlexSSEEventMap,
} from './plex-server/sse/plex-event-source.js'
import { SessionTracker } from './plex-server/sse/session-tracker.js'
import {
  type ContentScannedHandler,
  TimelineDebouncer,
} from './plex-server/sse/timeline-debouncer.js'

// HTTP timeout constants
const PLEX_API_TIMEOUT = 30000 // 30 seconds for Plex API operations

/** Convert XML boolean values ("0"/"1" strings or actual booleans) to boolean */
const xmlBool = (val: string | boolean | undefined): boolean | undefined => {
  if (val === undefined) return undefined
  if (typeof val === 'boolean') return val
  return val === '1'
}

/**
 * PlexServerService class for maintaining state and providing Plex operations
 */
export class PlexServerService {
  private readonly log: FastifyBaseLogger

  // Connection and server information cache
  private serverConnections: PlexServerConnectionInfo[] | null = null
  private serverMachineId: string | null = null
  private serverName: string | null = null

  // Plex Pass and admin identity (runtime-only, set during token validation)
  private _hasPlexPass: boolean | null = null
  private _adminPlexId: number | null = null
  private connectionTimestamp = 0
  private selectedConnectionUrl: string | null = null // Track which URL we've selected

  // User-related cache
  private users: PlexUser[] | null = null
  private usersTimestamp = 0
  // Server list cache - caches the raw Plex resources from plex.tv API
  private plexResourcesCache: PlexResource[] | null = null

  // Protection workflow cache — workflow-scoped, cleared after each delete-sync run
  private protectedItemsCache: Set<string> | null = null

  // Connection selection cache - TTL-based (survives across reconciliations)
  // Caches the best working connection for each server
  private serverConnectionCache: Map<string, CachedConnection> = new Map()

  // Content availability cache - reconciliation-scoped (no TTL, cleared at cycle start)
  // Caches which content exists on which servers within a single reconciliation
  private contentAvailabilityCache: Map<string, CachedContentAvailability> =
    new Map()

  // Dead server tracking for backoff - prevents hammering unavailable servers
  private deadServerCache: Map<string, number> = new Map()

  // Cache TTL constants
  private readonly CONNECTION_CACHE_TTL = 30 * 60 * 1000 // 30 minutes
  private readonly DEAD_SERVER_BACKOFF = 5 * 60 * 1000 // 5 minutes
  // Note: No CONTENT_CACHE_TTL - content cache is reconciliation-scoped

  // SSE connection and session tracking
  private eventSource: PlexEventSource | null = null
  private sessionTracker: SessionTracker | null = null
  private timelineDebouncer: TimelineDebouncer | null = null
  private staleSweepInterval: ReturnType<typeof setInterval> | null = null
  private static readonly STALE_SESSION_MS = 5 * 60 * 1000 // 5 minutes
  private static readonly STALE_SWEEP_INTERVAL_MS = 60 * 1000 // 60 seconds

  /**
   * Creates a new PlexServerService instance
   *
   * @param log - Fastify logger instance
   * @param fastify - Fastify instance for accessing configuration
   */
  constructor(
    readonly baseLog: FastifyBaseLogger,
    private readonly fastify: FastifyInstance,
  ) {
    this.log = createServiceLogger(baseLog, 'PLEX_SERVER')
    this.log.info('Initializing PlexServerService')
  }

  /**
   * Access to application configuration
   */
  private get config() {
    return this.fastify.config
  }

  getServerMachineId(): string | null {
    return this.serverMachineId
  }

  getServerName(): string | null {
    return this.serverName
  }

  getHasPlexPass(): boolean | null {
    return this._hasPlexPass
  }

  setHasPlexPass(value: boolean): void {
    this._hasPlexPass = value
  }

  getAdminPlexId(): number | null {
    return this._adminPlexId
  }

  setAdminPlexId(id: number): void {
    this._adminPlexId = id
  }

  /**
   * Retrieves the configured protection playlist name or returns default
   *
   * @returns The playlist name used for content protection
   */
  private getProtectionPlaylistName(): string {
    return this.config.plexProtectionListName || 'Do Not Delete'
  }

  // Track initialization state
  private initialized = false

  /**
   * Check if the service has been properly initialized
   *
   * @returns true if service is initialized, false otherwise
   */
  isInitialized(): boolean {
    return this.initialized
  }

  /**
   * Initializes the service by loading connections and users
   * Called during application startup to prepare the service
   *
   * @returns Promise that resolves to true if initialization succeeded, false otherwise
   */
  async initialize(): Promise<boolean> {
    try {
      this.log.info('Initializing PlexServerService connections and users')

      // Load server connections
      const connections = await this.getPlexServerConnectionInfo()
      if (!connections || connections.length === 0) {
        this.log.error(
          'Failed to initialize PlexServerService - no connections available',
        )
        this.initialized = false
        return false
      }

      // Load users
      const users = await this.getPlexUsers()
      if (!users || users.length === 0) {
        this.log.warn('No Plex users found during initialization')
      } else {
        this.log.debug(
          `Loaded ${users.length} Plex users during initialization`,
        )
      }

      this.initialized = true
      return true
    } catch (error) {
      this.log.error({ error }, 'Error initializing PlexServerService:')
      this.initialized = false
      return false
    }
  }

  /**
   * Retrieves and prioritizes Plex server connection details
   * Uses caching for performance optimization
   *
   * @returns Promise resolving to array of connection configurations
   */
  async getPlexServerConnectionInfo(): Promise<PlexServerConnectionInfo[]> {
    try {
      // Use cached connection data if valid (less than 15 minutes old)
      if (
        this.serverConnections &&
        Date.now() - this.connectionTimestamp < 15 * 60 * 1000
      ) {
        this.log.debug('Using cached Plex server connection info')
        return this.serverConnections
      }

      const plexTvUrl = 'https://plex.tv'
      const adminToken = this.config.plexTokens?.[0] || ''

      if (!adminToken) {
        this.log.warn('No Plex admin token available for connection discovery')
        return this.getDefaultConnectionInfo()
      }

      // Retrieve server resources from Plex.tv API
      const resourcesUrl = new URL('/api/v2/resources', plexTvUrl)
      resourcesUrl.searchParams.append('includeHttps', '1')
      const resourcesResponse = await fetch(resourcesUrl.toString(), {
        headers: {
          'User-Agent': USER_AGENT,
          Accept: 'application/json',
          'X-Plex-Token': adminToken,
          'X-Plex-Client-Identifier': PLEX_CLIENT_IDENTIFIER,
        },
        signal: AbortSignal.timeout(PLEX_API_TIMEOUT),
      })

      if (!resourcesResponse.ok) {
        throw new Error(
          `Failed to fetch resources: ${resourcesResponse.status} ${resourcesResponse.statusText}`,
        )
      }

      const resourcesData = (await resourcesResponse.json()) as PlexResource[]
      const serverResources = resourcesData.filter(
        (r) =>
          r.product === 'Plex Media Server' &&
          r.connections &&
          r.connections.length > 0,
      )

      if (serverResources.length === 0) {
        this.log.warn('No Plex server connections found, using default')
        return this.getDefaultConnectionInfo()
      }

      // Resolve which server the admin selected by matching plexServerUrl
      // against all server resources' connections
      const configUrl = this.config.plexServerUrl
      const defaultUrl = 'http://localhost:32400'

      let server: PlexResource | undefined
      if (configUrl && configUrl !== defaultUrl) {
        // Find the server whose connections include the configured URL
        for (const candidate of serverResources) {
          const match = candidate.connections.some(
            (conn) =>
              isSameServerEndpoint(conn.uri, configUrl) ||
              isSameServerEndpoint(
                `http://${conn.address}:${conn.port}`,
                configUrl,
              ),
          )
          if (match) {
            server = candidate
            this.log.debug(
              `Matched configured URL to server "${candidate.name}" (${candidate.clientIdentifier})`,
            )
            break
          }
        }

        if (!server) {
          // URL doesn't match any discovered server — fall back to first
          server = serverResources[0]
          this.log.warn(
            `Configured URL "${configUrl}" does not match any discovered server connection - falling back to "${server.name}"`,
          )
        }
      } else {
        // No manual URL configured — use first server
        server = serverResources[0]
        this.log.debug(
          `Using auto-discovered server "${server.name}" (no manual override)`,
        )
      }

      // Extract and categorize connections by priority
      const connections: PlexServerConnectionInfo[] = []

      for (const conn of server.connections) {
        connections.push({
          url: conn.uri,
          local: conn.local,
          relay: conn.relay,
          isDefault: false,
        })
      }

      // Sort connections by priority: non-relay first, then local first
      connections.sort((a, b) => {
        if (!a.relay && b.relay) return -1
        if (a.relay && !b.relay) return 1
        if (a.local && !b.local) return -1
        if (!a.local && b.local) return 1
        return 0
      })

      if (!(configUrl && configUrl !== defaultUrl) && connections.length > 0) {
        const candidates: ConnectionCandidate[] = connections.map((c) => ({
          uri: c.url,
          local: c.local,
          relay: c.relay,
        }))

        const reachable = await testConnectionReachability(
          candidates,
          adminToken,
          this.log,
        )

        if (reachable.length > 0) {
          const reachableUris = new Set(reachable.map((r) => r.uri))
          const filtered = connections.filter((c) => reachableUris.has(c.url))
          connections.splice(0, connections.length, ...filtered)
          this.log.info(
            `Filtered to ${connections.length} reachable connections (${candidates.length - connections.length} unreachable)`,
          )
        } else {
          this.log.warn(
            'All connection tests failed - keeping full list as fallback',
          )
        }
      }

      // Mark the first one as default
      if (connections.length > 0) {
        connections[0].isDefault = true
      }

      // If a manual URL is configured, promote it to default
      if (configUrl && configUrl !== defaultUrl) {
        const configMatch = connections.find((c) =>
          isSameServerEndpoint(c.url, configUrl),
        )

        if (configMatch) {
          for (const c of connections) {
            c.isDefault = false
          }
          configMatch.isDefault = true
          this.log.debug(
            'Manually configured URL matches a discovered connection - setting as default',
          )
        } else {
          // URL didn't match any server — add as manual override
          connections.push({
            url: configUrl,
            local: false,
            relay: false,
            isDefault: true,
          })

          for (let i = 0; i < connections.length - 1; i++) {
            connections[i].isDefault = false
          }

          this.log.debug(
            'Manually configured URL does not match any discovered connection - adding as override',
          )
        }
      }

      // Cache the result
      this.serverConnections = connections
      this.connectionTimestamp = Date.now()
      this.serverMachineId = server.clientIdentifier
      this.serverName = server.name

      // Check if manual config is being used
      const manualConfigUsed =
        this.config.plexServerUrl &&
        this.config.plexServerUrl !== 'http://localhost:32400'

      if (manualConfigUsed) {
        this.log.info(
          `Discovered ${connections.length} Plex server connections (manual config will be used)`,
        )
      } else {
        this.log.info(
          `Found ${connections.length} Plex server connections (${connections.filter((c) => c.local).length} local, ${connections.filter((c) => c.relay).length} relay)`,
        )
      }

      // Log connection details at info level for clear auto-configuration visibility
      if (connections.length > 0) {
        this.log.debug('Available Plex connections:')
        for (const [index, conn] of connections.entries()) {
          this.log.debug(
            `Connection ${index + 1}: URL=${conn.url}, Local=${conn.local}, Relay=${conn.relay}, Default=${conn.isDefault}`,
          )
        }
      }

      return connections
    } catch (error) {
      this.log.error({ error }, 'Error getting Plex server connection info:')
      return this.getDefaultConnectionInfo()
    }
  }

  /**
   * Returns a default connection configuration using the config value or localhost
   *
   * @returns Array containing a single default connection configuration
   */
  private getDefaultConnectionInfo(): PlexServerConnectionInfo[] {
    // Check if there's a manually configured URL that's not the default
    const configUrl = this.config.plexServerUrl
    const defaultUrl = 'http://localhost:32400'

    // Only use the configured URL if it's provided and not the default value
    if (configUrl && configUrl !== defaultUrl) {
      this.log.debug(
        `Using manually configured Plex URL as fallback: ${configUrl}`,
      )
      return [
        {
          url: configUrl,
          local:
            configUrl.includes('localhost') || configUrl.includes('127.0.0.1'),
          relay: false,
          isDefault: true,
        },
      ]
    }

    // Otherwise use localhost as the default fallback
    this.log.debug('Using localhost as default fallback Plex URL')
    return [
      {
        url: defaultUrl,
        local: true, // Localhost is always local
        relay: false,
        isDefault: true,
      },
    ]
  }

  /**
   * Selects the optimal Plex server URL for API calls based on priority
   *
   * @param preferLocal - Whether to prioritize local connections
   * @returns The best available Plex server URL
   */
  async getPlexServerUrl(preferLocal = true): Promise<string> {
    // If we've already selected a connection, reuse it without logging
    if (this.selectedConnectionUrl) {
      return this.selectedConnectionUrl
    }

    const connections = await this.getPlexServerConnectionInfo()

    if (connections.length === 0) {
      this.log.debug(
        'No Plex connections found, using localhost fallback: http://localhost:32400',
      )
      this.selectedConnectionUrl = 'http://localhost:32400'
      return this.selectedConnectionUrl
    }

    // Prioritize default connection if available
    const defaultConn = connections.find((c) => c.isDefault)
    if (defaultConn) {
      this.log.debug(`Using default Plex connection: ${defaultConn.url}`)
      this.selectedConnectionUrl = defaultConn.url
      return this.selectedConnectionUrl
    }

    // Otherwise if we prefer local and there's a local connection, use that
    if (preferLocal) {
      const localConn = connections.find((c) => c.local)
      if (localConn) {
        this.log.debug(`Using local Plex connection: ${localConn.url}`)
        this.selectedConnectionUrl = localConn.url
        return this.selectedConnectionUrl
      }
    }

    // Then try non-relay connections
    const nonRelayConn = connections.find((c) => !c.relay)
    if (nonRelayConn) {
      this.log.debug(`Using non-relay Plex connection: ${nonRelayConn.url}`)
      this.selectedConnectionUrl = nonRelayConn.url
      return this.selectedConnectionUrl
    }

    // Finally use the first available connection, even if it's a relay
    this.log.debug(
      `Using fallback Plex connection (relay): ${connections[0].url}`,
    )
    this.selectedConnectionUrl = connections[0].url
    return this.selectedConnectionUrl
  }

  /**
   * Retrieves Plex users with access to the configured server
   * Filters by serverMachineId to only return users for this server
   *
   * @param options.skipCache - Bypass the 30-minute cache for fresh data
   * @returns Promise resolving to array of Plex users
   */
  async getPlexUsers(options?: { skipCache?: boolean }): Promise<PlexUser[]> {
    try {
      // Use cached user data if valid (less than 30 minutes old)
      if (
        !options?.skipCache &&
        this.users &&
        Date.now() - this.usersTimestamp < 30 * 60 * 1000
      ) {
        this.log.debug('Using cached Plex users')
        return this.users
      }

      const plexTvUrl = 'https://plex.tv'
      const adminToken = this.config.plexTokens?.[0] || ''

      if (!adminToken) {
        this.log.warn('No Plex admin token available for user operations')
        return []
      }

      // Get all users including friends and home users
      const usersUrl = new URL('/api/users', plexTvUrl)
      const usersResponse = await fetch(usersUrl.toString(), {
        headers: {
          'X-Plex-Token': adminToken,
          'X-Plex-Client-Identifier': PLEX_CLIENT_IDENTIFIER,
        },
        signal: AbortSignal.timeout(PLEX_API_TIMEOUT),
      })

      if (!usersResponse.ok) {
        throw new Error(
          `Failed to fetch users: ${usersResponse.status} ${usersResponse.statusText}`,
        )
      }

      const responseText = await usersResponse.text()

      const xmlParser = new XMLParser({
        ignoreAttributes: false,
        attributeNamePrefix: '',
        isArray: (name) => name === 'User' || name === 'Server',
      })

      const parsed = xmlParser.parse(responseText)
      const allUsers = parsed.MediaContainer?.User || []

      this.log.debug(
        `Parsed ${allUsers.length} total users from Plex API response`,
      )

      // Filter to only users with access to the configured server
      const users = this.serverMachineId
        ? allUsers.filter(
            (user: { Server?: Array<{ machineIdentifier?: string }> }) => {
              const servers = user.Server || []
              return servers.some(
                (s) => s.machineIdentifier === this.serverMachineId,
              )
            },
          )
        : allUsers

      if (this.serverMachineId && users.length !== allUsers.length) {
        this.log.debug(
          `Filtered to ${users.length} users for server ${this.serverMachineId} (${allUsers.length - users.length} users on other servers excluded)`,
        )
      }

      // Format users into a consistent structure that matches the PlexUser interface
      const formattedUsers = users
        .map(
          (user: {
            id?: string
            username?: string
            title?: string
            email?: string
            thumb?: string
            home?: string | boolean
            restricted?: string | boolean
            protected?: string | boolean
            allowTuners?: string | number
            allowSync?: string | boolean
            allowCameraUpload?: string | boolean
            allowChannels?: string | boolean
            allowSubtitleAdmin?: string | boolean
            filterAll?: string
            filterMovies?: string
            filterMusic?: string
            filterPhotos?: string
            filterTelevision?: string
            Server?: Array<{
              id?: string
              serverId?: string
              machineIdentifier?: string
              name?: string
              lastSeenAt?: string
              numLibraries?: string | number
              allLibraries?: string | boolean
              owned?: string | boolean
              pending?: string | boolean
            }>
          }) => ({
            id: user.id || '',
            username: user.username || user.title || '',
            title: user.title || '',
            email: user.email || '',
            thumb: user.thumb,
            home: xmlBool(user.home),
            restricted: xmlBool(user.restricted),
            protected: xmlBool(user.protected),
            allowTuners:
              user.allowTuners != null ? Number(user.allowTuners) : undefined,
            allowSync: xmlBool(user.allowSync),
            allowCameraUpload: xmlBool(user.allowCameraUpload),
            allowChannels: xmlBool(user.allowChannels),
            allowSubtitleAdmin: xmlBool(user.allowSubtitleAdmin),
            filterAll: user.filterAll || undefined,
            filterMovies: user.filterMovies || undefined,
            filterMusic: user.filterMusic || undefined,
            filterPhotos: user.filterPhotos || undefined,
            filterTelevision: user.filterTelevision || undefined,
            Server: user.Server?.map((s) => ({
              id: s.id || '',
              serverId: s.serverId || '',
              machineIdentifier: s.machineIdentifier || '',
              name: s.name || '',
              lastSeenAt: s.lastSeenAt || '',
              numLibraries: s.numLibraries != null ? Number(s.numLibraries) : 0,
              allLibraries: xmlBool(s.allLibraries) ?? false,
              owned: xmlBool(s.owned) ?? false,
              pending: xmlBool(s.pending) ?? false,
            })),
          }),
        )
        .filter(
          (user: { id: string; title: string; username?: string }) =>
            !!user.id && (!!user.username || !!user.title),
        ) as PlexUser[]

      // Cache the result and return
      this.users = formattedUsers
      this.usersTimestamp = Date.now()

      this.log.debug(`Found ${formattedUsers.length} Plex users`)
      return formattedUsers
    } catch (error) {
      this.log.error({ error }, 'Error fetching Plex users:')
      return []
    }
  }

  // ── Playlist protection methods removed (CVE-2025-69417 migration) ──
  // getSharedServerInfo, getUserToken, findUserPlaylistByTitle,
  // createUserPlaylist, getOrCreateProtectionPlaylists, getUserPlaylistItems
  // were deleted. Protection now uses Plex Lists via getCustomListsForUser
  // with the admin token — no per-user tokens required.


  /**
   * Retrieves all protected item GUIDs from all user protection playlists
   * Fetches complete metadata for each item and extracts standardized GUIDs
   *
   * @returns Promise resolving to a set of protected GUIDs
   */
  async getProtectedItems(): Promise<Set<string>> {
    if (this.protectedItemsCache) {
      this.log.debug('Using cached protected items from current workflow')
      return this.protectedItemsCache
    }

    const protectedGuids = new Set<string>()

    if (!this.config.enablePlexListProtection) {
      this.log.debug('Plex list protection is disabled')
      return protectedGuids
    }

    const listName = this.getProtectionPlaylistName()
    const adminToken = this.config.plexTokens?.[0] || ''

    try {
      const users = await this.fastify.db.getAllUsers()
      const eligibleUsers = users.filter((u) => u.plex_uuid)

      if (eligibleUsers.length === 0) {
        this.log.warn(
          `Plex list protection is enabled but no users have a plex_uuid — ` +
            `protection list "${listName}" cannot be queried. ` +
            `Ensure users have synced their Plex watchlist at least once.`,
        )
        return protectedGuids
      }

      for (const user of eligibleUsers) {
        const friend = {
          watchlistId: user.plex_uuid as string,
          username: user.name,
          userId: user.id,
        }

        try {
          const items = await getCustomListsForUser(
            this.log,
            adminToken,
            friend,
            listName,
          )

          if (items.length === 0) {
            this.log.debug(
              `No protection list "${listName}" found for user "${user.name}"`,
            )
            continue
          }

          this.log.debug(
            `Processing ${items.length} protected items from list "${listName}" for user "${user.name}"`,
          )

          for (const item of items) {
            const itemMetadata = await this.getItemMetadata(
              user.name,
              item.id,
              undefined,
              item.type,
            )

            if (itemMetadata?.guids && itemMetadata.guids.length > 0) {
              for (const guid of itemMetadata.guids) {
                protectedGuids.add(guid)
              }
              this.log.debug(
                `Added protected item "${item.title}" with ${itemMetadata.guids.length} GUIDs`,
              )
            } else {
              this.log.warn(
                `Failed to resolve GUIDs for protected item "${item.title}" - item may not be properly protected`,
              )
            }
          }
        } catch (userError) {
          this.log.error(
            { error: userError },
            `Error fetching protection list for user "${user.name}" - aborting to prevent unprotected deletion`,
          )
          throw userError
        }
      }

      this.log.info(
        `Found ${protectedGuids.size} unique protected GUIDs across all users`,
      )

      this.protectedItemsCache = protectedGuids
      return protectedGuids
    } catch (error) {
      this.log.error(
        { error },
        'Error getting protected items from Plex Lists:',
      )
      throw error
    }
  }

  /**
   * Retrieves comprehensive metadata for a Plex item including standardized GUIDs
   *
   * @param username - The Plex username to authenticate as
   * @param plexGuid - The Plex GUID in format "plex://movie/5d776832a091de001f2e780f" or "plex://episode/5ea3e26f382f910042f103d0"
   * @param grandparentGuid - For TV episodes, the show's GUID in format "plex://show/5eb6b5ffac1f29003f4a737b"
   * @param itemType - The type of the item ("movie", "show", "episode")
   * @returns Promise resolving to an object with title and GUIDs, or null if not found
   */
  async getItemMetadata(
    _username: string,
    plexGuid: string,
    grandparentGuid?: string,
    itemType?: string,
  ): Promise<{ title: string; guids: string[] } | null> {
    try {
      // For TV shows, use the grandparentGuid (show GUID) if available
      const guidToUse =
        itemType === 'episode' && grandparentGuid ? grandparentGuid : plexGuid

      // Extract the media ID from the Plex GUID to create a key
      const mediaId = guidToUse.split(/[/:]/).pop()
      if (!mediaId) {
        this.log.warn(`Invalid Plex GUID format: "${guidToUse}"`)
        return null
      }

      // Determine the content type from the GUID
      const contentType = guidToUse.includes('/movie/')
        ? 'movie'
        : guidToUse.includes('/show/')
          ? 'show'
          : itemType || (plexGuid.includes('/episode/') ? 'show' : 'movie')

      // Create a temporary item structure for metadata retrieval
      const tempItem = {
        id: mediaId,
        key: mediaId,
        title: itemType === 'episode' ? 'TV Episode' : 'Protected Item',
        type: contentType,
        user_id: 0,
        status: 'pending' as const,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        guids: [],
        genres: [],
      }

      // No pre-check needed, letting toItemsSingle handle the metadata retrieval and retries

      // Utilize toItemsSingle utility for standardized GUID extraction
      const itemSet = await toItemsSingle(
        this.config,
        this.log,
        tempItem,
        0, // start with retry count 0
        3, // Allow standard retry count for metadata retrieval
      )

      // Extract first result from the metadata set
      const items = Array.from(itemSet)
      if (items.length === 0) {
        this.log.warn('No metadata found for item')
        return null
      }

      const item = items[0] as Item

      // Extract standardized GUIDs and ensure we return a valid array
      const extractedGuids = Array.isArray(item.guids)
        ? item.guids
        : typeof item.guids === 'string'
          ? parseGuids(item.guids)
          : []

      // Log the found GUIDs at debug level
      if (extractedGuids.length > 0) {
        this.log.debug(
          `Found ${extractedGuids.length} GUIDs for item "${item.title || 'Unknown'}"`,
        )
      } else {
        this.log.warn(
          `No standardized GUIDs found for item "${item.title || 'Unknown'}"`,
        )
      }

      return {
        title: item.title || `Unknown ${itemType || 'item'}`,
        guids: extractedGuids,
      }
    } catch (error) {
      this.log.error({ error }, 'Error getting metadata for item')
      return null
    }
  }

  /**
   * Determines if an item is protected by any user's protection playlist
   *
   * @param itemGuids - The GUIDs of the item to check, can be a string, array, or undefined
   * @param itemTitle - Optional title for better logging
   * @returns True if item is protected, false otherwise
   */
  async isItemProtected(
    itemGuids: string[] | string | undefined,
    itemTitle?: string,
  ): Promise<boolean> {
    // Early return if protection is disabled
    if (!this.config.enablePlexListProtection) {
      this.log.debug(
        'Plex playlist protection is disabled - skipping protection check',
      )
      return false
    }

    // Validate input GUIDs
    if (
      !itemGuids ||
      (Array.isArray(itemGuids) && itemGuids.length === 0) ||
      (typeof itemGuids === 'string' && !itemGuids.trim())
    ) {
      this.log.warn(
        `No GUIDs provided to protection check${itemTitle ? ` for "${itemTitle}"` : ''}`,
      )
      return false
    }

    // Get all protected GUIDs
    const protectedGuids = await this.getProtectedItems()
    if (protectedGuids.size === 0) {
      this.log.debug('No protected items found in any user playlist')
      return false
    }

    // Parse the input GUIDs to standardized format
    const parsedGuids = parseGuids(itemGuids)
    if (parsedGuids.length === 0) {
      this.log.warn(
        `No valid GUIDs found in input for item${itemTitle ? ` "${itemTitle}"` : ''}`,
      )
      return false
    }

    // Check for any matching GUIDs against the protected set
    for (const guid of parsedGuids) {
      if (protectedGuids.has(guid)) {
        this.log.info(
          `Item${itemTitle ? ` "${itemTitle}"` : ''} is protected with matching GUID: "${guid}"`,
        )
        return true
      }
    }

    // For debugging, log the GUIDs we checked
    if (this.log.level === 'debug' || this.log.level === 'trace') {
      this.log.debug(
        `Item${itemTitle ? ` "${itemTitle}"` : ''} with GUIDs [${parsedGuids.join(', ')}] is not protected`,
      )
    }
    return false
  }

  /**
   * Resets all cached data to force fresh retrieval
   * Useful for testing or when manual refresh is required
   *
   * @param resetInitialized - If true, will also reset the initialized state (default: false)
   */
  clearCaches(resetInitialized = false): void {
    this.log.debug('Clearing all PlexServerService caches')
    this.serverConnections = null
    this.serverMachineId = null
    this.serverName = null
    this._hasPlexPass = null
    this._adminPlexId = null
    this.connectionTimestamp = 0
    this.selectedConnectionUrl = null
    this.users = null
    this.usersTimestamp = 0
    this.protectedItemsCache = null
    this.plexResourcesCache = null

    // Clear connection and content caches
    this.serverConnectionCache.clear()
    this.contentAvailabilityCache.clear()
    this.deadServerCache.clear()

    // Only reset the initialized state if explicitly requested
    if (resetInitialized) {
      this.log.warn('Resetting Plex server initialization state')
      this.initialized = false
    }
  }

  /**
   * Connect to Plex SSE notification endpoint for real-time event delivery.
   * Polling jobs remain as a safety net - SSE provides faster reaction times.
   */
  async connectSSE(): Promise<void> {
    const serverUrl = await this.getPlexServerUrl()
    const token = this.config.plexTokens?.[0] || ''

    if (!token) {
      this.log.warn('No Plex token available, skipping SSE connection')
      return
    }

    this.sessionTracker = new SessionTracker(this.log)
    this.timelineDebouncer = new TimelineDebouncer()

    this.eventSource = new PlexEventSource({
      serverUrl,
      token,
      logger: this.log,
    })

    this.eventSource.on('timeline', (entries) => {
      this.timelineDebouncer?.handleTimelineEntries(entries)
    })

    this.eventSource.on('connected', () => {
      this.log.info('SSE connected - reconciling with live sessions')
      void this.reconcileSessionsOnConnect()
    })

    this.eventSource.on('disconnected', () => {
      this.log.warn('SSE disconnected - polling continues as fallback')
    })

    // Start stale session sweep
    this.staleSweepInterval = setInterval(() => {
      if (this.sessionTracker) {
        const stale = this.sessionTracker.sweepStale(
          PlexServerService.STALE_SESSION_MS,
        )
        if (stale.length > 0) {
          this.log.info(
            { count: stale.length },
            'Swept stale sessions from SSE tracker',
          )
        }
      }
    }, PlexServerService.STALE_SWEEP_INTERVAL_MS)

    await this.eventSource.connect()
  }

  /**
   * Disconnect SSE and clean up all related resources.
   */
  disconnectSSE(): void {
    if (this.staleSweepInterval) {
      clearInterval(this.staleSweepInterval)
      this.staleSweepInterval = null
    }
    if (this.timelineDebouncer) {
      this.timelineDebouncer.destroy()
      this.timelineDebouncer = null
    }
    if (this.eventSource) {
      this.eventSource.disconnect()
      this.eventSource.removeAllListeners()
      this.eventSource = null
    }
    if (this.sessionTracker) {
      this.sessionTracker.clear()
      this.sessionTracker = null
    }
  }

  /**
   * Subscribe to SSE events emitted by the Plex connection.
   */
  onSSE<K extends keyof PlexSSEEventMap>(
    event: K,
    handler: (...args: PlexSSEEventMap[K]) => void,
  ): void {
    this.eventSource?.on(event, handler)
  }

  /**
   * Check if the SSE connection to Plex is currently active.
   * Used by polling jobs to skip redundant work when SSE delivers events in real time.
   */
  isSSEConnected(): boolean {
    return this.eventSource?.isConnected() ?? false
  }

  /**
   * Unsubscribe from SSE events.
   */
  offSSE<K extends keyof PlexSSEEventMap>(
    event: K,
    handler: (...args: PlexSSEEventMap[K]) => void,
  ): void {
    this.eventSource?.off(event, handler)
  }

  /**
   * Get the session tracker instance for SSE playing event filtering.
   */
  getSessionTracker(): SessionTracker | null {
    return this.sessionTracker
  }

  /**
   * Subscribe to debounced "content scanned" events from Plex timeline SSE.
   * Fires after a 2-second quiet period following state-5 timeline entries.
   */
  onContentScanned(handler: ContentScannedHandler): void {
    this.timelineDebouncer?.onContentScanned(handler)
  }

  /**
   * On SSE reconnect, hydrate the session tracker with any sessions that
   * started while we were disconnected.
   */
  private async reconcileSessionsOnConnect(): Promise<void> {
    try {
      const liveSessions = await this.getActiveSessions()
      if (liveSessions.length === 0) return

      const added = this.sessionTracker?.hydrate(liveSessions) ?? 0
      if (added > 0) {
        this.log.info(
          { total: liveSessions.length, added },
          'Hydrated session tracker from live sessions on SSE connect',
        )
      }
    } catch (error) {
      this.log.warn({ error }, 'Failed to reconcile sessions on SSE connect')
    }
  }

  /**
   * Clears only the workflow-specific caches
   * Should be called at the end of a delete sync workflow
   */
  clearWorkflowCaches(): void {
    this.log.debug('Clearing workflow-specific caches')
    this.protectedItemsCache = null
  }

  /**
   * Clears the Plex resources cache
   *
   * Should be called at the start of reconciliation to ensure fresh server list
   */
  clearPlexResourcesCache(): void {
    this.plexResourcesCache = null
    this.log.debug('Cleared Plex resources cache')
  }

  /**
   * Retrieves active Plex sessions from the server
   *
   * @returns Promise resolving to array of active sessions
   */
  async getActiveSessions(): Promise<PlexSession[]> {
    const serverUrl = await this.getPlexServerUrl()
    const token = this.config.plexTokens?.[0] || ''
    return getActiveSessions(serverUrl, token, this.log)
  }

  /**
   * Retrieves detailed show metadata including season and episode information
   *
   * @param ratingKey - The show's rating key
   * @param includeChildren - Whether to include season/episode details
   * @returns Promise resolving to show metadata or null
   */
  async getShowMetadata(
    ratingKey: string,
    includeChildren: true,
  ): Promise<PlexShowMetadata | null>
  async getShowMetadata(
    ratingKey: string,
    includeChildren: false,
  ): Promise<PlexShowMetadataResponse | null>
  async getShowMetadata(
    ratingKey: string,
    includeChildren = true,
  ): Promise<PlexShowMetadata | PlexShowMetadataResponse | null> {
    const serverUrl = await this.getPlexServerUrl()
    const token = this.config.plexTokens?.[0] || ''
    return getShowMetadata(
      ratingKey,
      includeChildren,
      serverUrl,
      token,
      this.log,
    )
  }

  /**
   * Searches for content in the Plex library by GUID
   *
   * @param guid - The GUID to search for (will be normalized)
   * @returns Promise resolving to array of matching PlexMetadata items
   */
  async searchByGuid(guid: string): Promise<PlexMetadata[]> {
    const serverUrl = await this.getPlexServerUrl()
    const token = this.config.plexTokens?.[0] || ''
    return searchByGuid(guid, serverUrl, token, this.log)
  }

  /**
   * Retrieves direct children of a library item via /library/metadata/{id}/children
   *
   * For a show, returns seasons. For a season, returns episodes.
   */
  async getMetadataChildren(
    ratingKey: string,
  ): Promise<PlexChildrenResponse | null> {
    const serverUrl = await this.getPlexServerUrl()
    const token = this.config.plexTokens?.[0] || ''
    return getMetadataChildren(ratingKey, serverUrl, token, this.log)
  }

  /**
   * Clears the content cache at the start of each reconciliation.
   * Content cache is reconciliation-scoped, not TTL-based.
   * Called from watchlist-workflow.service.ts at the start of syncWatchlistItems().
   */
  clearContentCacheForReconciliation(): void {
    clearContentCacheForReconciliation(this.contentAvailabilityCache, this.log)
  }

  /**
   * Checks if the owner's Plex server is reachable via the /identity endpoint.
   * Used as a pre-flight check before processing watchlist items when
   * skipIfExistsOnPlex is enabled - if the primary server is down, we can't
   * trust "not found" results and must abort to prevent mass-routing.
   *
   * @returns Promise resolving to health status with reachable flag
   */
  async checkPlexServerHealth(): Promise<{
    reachable: boolean
    serverName: string | null
  }> {
    try {
      const ownerConnections = await this.getPlexServerConnectionInfo()

      if (ownerConnections.length === 0) {
        this.log.warn('No owner server connections available for health check')
        return { reachable: false, serverName: this.serverName }
      }

      const candidates: ConnectionCandidate[] = ownerConnections.map((c) => ({
        uri: c.url,
        local: c.local,
        relay: c.relay,
      }))

      const reachable = await testConnectionReachability(
        candidates,
        this.config.plexTokens?.[0] || '',
        this.log,
      )

      if (reachable.length === 0) {
        this.log.warn(
          { serverName: this.serverName },
          'Plex server health check failed - no connections reachable',
        )
        return { reachable: false, serverName: this.serverName }
      }

      // /identity only confirms the HTTP server is up. During startup
      // maintenance or DB migrations, Plex returns 503 on authenticated
      // endpoints and library queries return empty results. Probe
      // /library/sections to verify the library is actually queryable.
      const token = this.config.plexTokens?.[0] || ''
      const serverUri = reachable[0].uri
      const libraryReady = await this.waitForLibraryReady(serverUri, token)

      if (!libraryReady) {
        this.log.warn(
          { serverName: this.serverName },
          'Plex server is reachable but library is not ready (maintenance or still starting)',
        )
        return { reachable: false, serverName: this.serverName }
      }

      this.log.debug(
        { serverName: this.serverName, reachableCount: reachable.length },
        'Plex server health check passed',
      )
      return { reachable: true, serverName: this.serverName }
    } catch (error) {
      this.log.error(
        { error, serverName: this.serverName },
        'Error during Plex server health check',
      )
      return { reachable: false, serverName: this.serverName }
    }
  }

  /**
   * Polls /library/sections until the library is queryable or the retry
   * budget is exhausted. Plex returns 503 during startup maintenance and
   * DB migrations - hitting the library in that state causes empty results
   * that look like "content not found" and triggers false routing.
   */
  private async waitForLibraryReady(
    serverUri: string,
    token: string,
    maxAttempts = 12,
    intervalMs = 5000,
  ): Promise<boolean> {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        const response = await fetch(`${serverUri}/library/sections`, {
          headers: {
            Accept: 'application/json',
            'X-Plex-Token': token,
            'X-Plex-Client-Identifier': PLEX_CLIENT_IDENTIFIER,
          },
          signal: AbortSignal.timeout(5000),
        })

        if (response.status === 503) {
          this.log.info(
            { attempt, maxAttempts, serverName: this.serverName },
            'Plex server in maintenance mode, waiting for library to become ready',
          )
          await new Promise((resolve) => setTimeout(resolve, intervalMs))
          continue
        }

        if (response.status === 401 || response.status === 403) {
          this.log.error(
            { status: response.status, serverName: this.serverName },
            'Plex library probe failed due to invalid or unauthorized token',
          )
          return false
        }

        if (!response.ok) {
          this.log.warn(
            { status: response.status, attempt },
            'Unexpected response from /library/sections',
          )
          await new Promise((resolve) => setTimeout(resolve, intervalMs))
          continue
        }

        const data = (await response.json()) as {
          MediaContainer?: { Directory?: Array<{ key: string }> }
        }

        const sections = data?.MediaContainer?.Directory ?? []
        if (sections.length > 0) {
          if (attempt > 1) {
            this.log.info(
              { attempt, sectionCount: sections.length },
              'Plex library is now ready',
            )
          }
          return true
        }

        this.log.info(
          { attempt, maxAttempts },
          'Plex returned no library sections, waiting for library to load',
        )
        await new Promise((resolve) => setTimeout(resolve, intervalMs))
      } catch (error) {
        this.log.debug(
          { error, attempt, maxAttempts },
          'Error probing /library/sections',
        )
        await new Promise((resolve) => setTimeout(resolve, intervalMs))
      }
    }

    return false
  }

  /**
   * Checks if content exists across accessible Plex servers using the primary token.
   * Uses cached connections (one per server) and reconciliation-scoped content cache
   * for efficient checking across multiple items.
   *
   * @param plexKey - The Plex GUID part (e.g., "5d7768376f4521001ea9c9ad")
   * @param contentType - The content type ("movie" or "show")
   * @param isPrimaryUser - Whether the requesting user is the primary token user (server owner)
   *                        - If true: checks owner's server + all shared servers
   *                        - If false: checks only owner's server
   * @returns Promise<boolean> true if found on any accessible server, false otherwise
   */
  async checkExistenceAcrossServers(
    plexKey: string | undefined,
    contentType: 'movie' | 'show',
    isPrimaryUser: boolean,
  ): Promise<boolean> {
    if (!plexKey) {
      this.log.debug('No Plex key provided for existence check')
      return false
    }

    try {
      const adminToken = this.config.plexTokens?.[0] || ''

      if (!adminToken) {
        this.log.warn(
          'No Plex admin token available for multi-server existence check',
        )
        return false
      }

      // Fetch all accessible servers from plex.tv API (with caching)
      let allResources = this.plexResourcesCache
      if (!allResources) {
        this.log.debug('Server resources cache miss, fetching from plex.tv API')
        allResources = await this.getAllPlexResources(adminToken)
        this.plexResourcesCache = allResources
      } else {
        this.log.debug('Using cached server resources')
      }

      // Get owner's server connections (respects plexServerUrl setting)
      const ownerConnections = await this.getPlexServerConnectionInfo()

      // Build list of unique servers (not connections) to check
      const serversToCheck = buildUniqueServerList(
        ownerConnections,
        allResources,
        adminToken,
        isPrimaryUser,
        { logger: this.log, serverMachineId: this.serverMachineId },
      )

      if (serversToCheck.length === 0) {
        this.log.debug('No Plex servers found for existence check')
        return false
      }

      const plexGuid = buildPlexGuid(contentType, plexKey)

      this.log.debug(
        {
          plexKey,
          contentType,
          plexGuid,
          serverCount: serversToCheck.length,
        },
        'Checking content existence across Plex servers',
      )

      // Prepare dependencies for module functions
      const connectionCacheDeps = {
        logger: this.log,
        connectionCacheTtl: this.CONNECTION_CACHE_TTL,
        deadServerBackoff: this.DEAD_SERVER_BACKOFF,
      }
      const contentCacheDeps = { logger: this.log }

      // Check all servers in parallel with AbortController for early termination
      const abortController = new AbortController()

      const serverChecks = serversToCheck.map(async (server) => {
        // Get the best cached connection for this server
        const connection = await getBestServerConnection(
          server.clientIdentifier,
          server.name,
          server.connections,
          server.accessToken,
          this.serverConnectionCache,
          this.deadServerCache,
          connectionCacheDeps,
        )

        if (!connection) {
          this.log.debug(
            `No working connection for server "${server.name}", skipping`,
          )
          return { server: server.name, found: false }
        }

        // Check content on this server (uses content cache)
        const found = await checkContentOnServer(
          server.clientIdentifier,
          server.name,
          connection.uri,
          connection.accessToken,
          plexGuid,
          contentType,
          this.contentAvailabilityCache,
          this.serverConnectionCache,
          contentCacheDeps,
          abortController.signal,
        )

        if (found) {
          this.log.info(
            `Content found on Plex server "${server.name}" - skipping download`,
          )
          // Cancel other pending requests
          abortController.abort()
        }

        return { server: server.name, found }
      })

      // Wait for all checks to complete (or abort early)
      const results = await Promise.allSettled(serverChecks)

      // Check if any server found the content
      const foundOnAnyServer = results.some(
        (result) => result.status === 'fulfilled' && result.value.found,
      )

      if (!foundOnAnyServer) {
        this.log.debug('Content not found on any accessible Plex server')
      }

      return foundOnAnyServer
    } catch (error) {
      this.log.error(
        { error, plexKey, contentType },
        'Error checking Plex servers for content existence',
      )
      // On error, return false to allow download (fail open)
      return false
    }
  }

  /**
   * Fetches all Plex resources (servers) from plex.tv API
   *
   * @param token - The Plex token to use for authentication
   * @returns Array of PlexResource objects
   */
  private async getAllPlexResources(token: string): Promise<PlexResource[]> {
    return getAllPlexResources(token, this.log)
  }

  /**
   * Retrieves detailed metadata for a specific item by rating key
   *
   * @param ratingKey - The Plex rating key of the item
   * @returns Promise resolving to metadata or null if not found
   */
  async getMetadata(ratingKey: string): Promise<PlexMetadata | null> {
    const serverUrl = await this.getPlexServerUrl()
    const token = this.config.plexTokens?.[0] || ''
    return getMetadata(ratingKey, serverUrl, token, this.log)
  }

  /**
   * Gets current labels for a specific Plex item
   *
   * @param ratingKey - The Plex rating key of the item
   * @returns Promise resolving to array of current label strings, or empty array if none found
   */
  async getCurrentLabels(ratingKey: string): Promise<string[]> {
    const serverUrl = await this.getPlexServerUrl()
    const token = this.config.plexTokens?.[0] || ''
    return getCurrentLabels(ratingKey, serverUrl, token, this.log)
  }

  /**
   * Removes specific labels from a Plex item by updating with filtered labels
   *
   * @param ratingKey - The Plex rating key of the item
   * @param labelsToRemove - Array of label strings to remove from the item
   * @returns Promise resolving to true if successful, false otherwise
   */
  async removeSpecificLabels(
    ratingKey: string,
    labelsToRemove: string[],
  ): Promise<boolean> {
    const serverUrl = await this.getPlexServerUrl()
    const token = this.config.plexTokens?.[0] || ''
    return removeSpecificLabels(
      ratingKey,
      labelsToRemove,
      serverUrl,
      token,
      this.log,
    )
  }

  /**
   * Updates the labels for a specific Plex item
   *
   * @param ratingKey - The Plex rating key of the item to update
   * @param labels - Array of label strings to set on the item
   * @returns Promise resolving to true if successful, false otherwise
   */
  async updateLabels(ratingKey: string, labels: string[]): Promise<boolean> {
    const serverUrl = await this.getPlexServerUrl()
    const token = this.config.plexTokens?.[0] || ''
    return updateLabels(ratingKey, labels, serverUrl, token, this.log)
  }
}
