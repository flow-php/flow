<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

enum MigrationState: string
{
    case EXECUTED = 'executed';
    case PENDING = 'pending';
    case UNAVAILABLE = 'unavailable';
}
