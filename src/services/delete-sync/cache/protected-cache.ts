import type { FastifyBaseLogger, FastifyInstance } from 'fastify'

/**
 * Ensures protection cache is loaded once per workflow
 * Avoids redundant API calls for protection playlist loading
 *
 * @param currentCache - The current protection cache (null if not loaded)
 * @param enabled - Whether protection is enabled
 * @param fastify - Fastify instance for accessing Plex server service
 * @param playlistName - Name of the protection playlist
 * @param logger - Logger instance for debug/error messages
 * @returns Set of protected GUIDs, or null if protection is disabled
 */
export async function ensureProtectionCache(
  currentCache: Set<string> | null,
  enabled: boolean,
  fastify: FastifyInstance,
  _playlistName: string,
  logger: FastifyBaseLogger,
): Promise<Set<string> | null> {
  if (!enabled) {
    return null
  }

  if (currentCache !== null) {
    return currentCache
  }

  if (!fastify.plexServerService.isInitialized()) {
    throw new Error(
      'Plex server not initialized for protection playlist access',
    )
  }

  try {
    logger.debug('Loading protection lists and caching results...')

    const protectedGuids = await fastify.plexServerService.getProtectedItems()

    logger.debug(
      `Cached ${protectedGuids.size} protected item GUIDs from Plex Lists`,
    )

    return protectedGuids
  } catch (error) {
    logger.error({ error }, 'Error loading protection lists for caching')
    throw error
  }
}

/**
 * Returns true if any of the provided GUIDs exist in the protected set.
 * Optional onHit callback lets callers log the first matching GUID.
 *
 * @param guidList - Array of GUIDs to check
 * @param protectedGuids - Set of protected GUIDs
 * @param enabled - Whether protection is enabled
 * @param onHit - Optional callback when a match is found
 * @returns True if any GUID is protected
 */
export function isAnyGuidProtected(
  guidList: string[],
  protectedGuids: Set<string> | null,
  enabled: boolean,
  onHit?: (guid: string) => void,
): boolean {
  if (!enabled || !protectedGuids) {
    return false
  }
  for (const guid of guidList) {
    if (protectedGuids.has(guid)) {
      onHit?.(guid)
      return true
    }
  }
  return false
}
