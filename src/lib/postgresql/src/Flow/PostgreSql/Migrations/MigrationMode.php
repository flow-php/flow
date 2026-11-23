<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

enum MigrationMode: string
{
    case ALL_PENDING = 'all_pending';
    case STRICT_ORDER = 'strict_order';
}
