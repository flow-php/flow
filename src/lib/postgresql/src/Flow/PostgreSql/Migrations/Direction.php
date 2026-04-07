<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

enum Direction : string
{
    case DOWN = 'down';
    case UP = 'up';
}
