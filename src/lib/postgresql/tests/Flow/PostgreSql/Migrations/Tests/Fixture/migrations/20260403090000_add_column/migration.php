<?php

declare(strict_types=1);

use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\MigrationContext;

return new class implements Migration {
    public function migrate(MigrationContext $context): void
    {
        $context->client->execute('ALTER TABLE users ADD COLUMN email VARCHAR(255)');
    }

    public function transactional(): bool
    {
        return true;
    }
};
