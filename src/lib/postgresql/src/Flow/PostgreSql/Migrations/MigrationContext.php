<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

use Flow\PostgreSql\Client\Client;

final readonly class MigrationContext
{
    public function __construct(
        public Client $client,
    ) {}
}
