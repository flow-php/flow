<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

interface VersionGenerator
{
    public function generate(): Version;
}
