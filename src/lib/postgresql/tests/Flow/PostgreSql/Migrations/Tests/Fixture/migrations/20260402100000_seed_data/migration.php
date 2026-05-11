<?php

declare(strict_types=1);

use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\MigrationContext;

return new class implements Migration {
    public function migrate(MigrationContext $context): void
    {
        $context->client->execute("INSERT INTO users (name) VALUES ('seed')");
    }

    public function transactional(): bool
    {
        return true;
    }
};
