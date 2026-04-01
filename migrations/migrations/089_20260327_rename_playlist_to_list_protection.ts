import type { Knex } from 'knex'

/**
 * Renames Plex playlist protection config columns to Plex List protection.
 *
 * The protection feature migrated from Plex playlists (broken by CVE-2025-69417,
 * PMS 4.147.1+) to Plex Lists, which use the admin-token GraphQL pattern and
 * require no per-user server tokens.
 *
 * Column renames:
 * - configs: `enablePlexPlaylistProtection` → `enablePlexListProtection`
 * - configs: `plexProtectionPlaylistName` → `plexProtectionListName`
 */
export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable('configs', (table) => {
    table.renameColumn(
      'enablePlexPlaylistProtection',
      'enablePlexListProtection',
    )
  })

  await knex.schema.alterTable('configs', (table) => {
    table.renameColumn('plexProtectionPlaylistName', 'plexProtectionListName')
  })
}

/**
 * Reverts the column renames back to playlist naming.
 */
export async function down(knex: Knex): Promise<void> {
  await knex.schema.alterTable('configs', (table) => {
    table.renameColumn(
      'enablePlexListProtection',
      'enablePlexPlaylistProtection',
    )
  })

  await knex.schema.alterTable('configs', (table) => {
    table.renameColumn('plexProtectionListName', 'plexProtectionPlaylistName')
  })
}
