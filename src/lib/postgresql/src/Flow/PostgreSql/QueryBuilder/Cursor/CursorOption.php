<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Cursor;

/**
 * PostgreSQL cursor options (bitmask values from src/include/nodes/parsenodes.h).
 *
 * CURSOR_OPT_BINARY    = 0x0001
 * CURSOR_OPT_SCROLL    = 0x0002
 * CURSOR_OPT_NO_SCROLL = 0x0004
 * CURSOR_OPT_INSENSITIVE = 0x0008 (always on by default in PG)
 * CURSOR_OPT_ASENSITIVE = 0x0010 (PostgreSQL 17+)
 * CURSOR_OPT_HOLD      = 0x0020
 * CURSOR_OPT_FAST_PLAN = 0x0100
 * CURSOR_OPT_GENERIC_PLAN = 0x0200
 * CURSOR_OPT_CUSTOM_PLAN = 0x0400
 * CURSOR_OPT_PARALLEL_OK = 0x0800
 */
final class CursorOption
{
    public const int BINARY = 0x0001;

    public const int HOLD = 0x0020;

    public const int NO_SCROLL = 0x0004;

    public const int SCROLL = 0x0002;
}
