<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

enum VersionAlias : string
{
    case FIRST = 'first';
    case LATEST = 'latest';
    case NEXT = 'next';
    case PREV = 'prev';
}
