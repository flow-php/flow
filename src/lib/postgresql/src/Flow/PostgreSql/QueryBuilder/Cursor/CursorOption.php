<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Cursor;

/**
 * Bitmask values from src/include/nodes/parsenodes.h.
 */
final class CursorOption
{
    public const int BINARY = 0x0001;

    public const int HOLD = 0x0020;

    public const int NO_SCROLL = 0x0004;

    public const int SCROLL = 0x0002;
}
