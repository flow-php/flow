<?php

declare(strict_types=1);

use Flow\PostgreSql\Migrations\{MigrationContext, Rollback};

return new class implements Rollback {
    public function rollback(MigrationContext $context) : void
    {
        $context->client->execute("DELETE FROM users WHERE name = 'seed'");
    }

    public function transactional() : bool
    {
        return true;
    }
};
