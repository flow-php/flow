<?php

declare(strict_types=1);

use Flow\PostgreSql\Migrations\MigrationContext;
use Flow\PostgreSql\Migrations\Rollback;

return new class implements Rollback {
    public function rollback(MigrationContext $context): void
    {
        $context->client->execute('DROP TABLE users');
    }

    public function transactional(): bool
    {
        return true;
    }
};
