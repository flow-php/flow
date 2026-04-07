<?php

declare(strict_types=1);

use Flow\PostgreSql\Migrations\{Migration, MigrationContext};

return new class implements Migration {
    public function migrate(MigrationContext $context) : void
    {
        $context->client->execute('CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)');
    }

    public function transactional() : bool
    {
        return true;
    }
};
