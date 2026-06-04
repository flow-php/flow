<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\MigrationSeedProvider;
use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\MigrationContext;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;

return new class implements Migration {
    public function migrate(MigrationContext $context): void
    {
        $table = type_string()->assert($context->attribute('table_name'));
        $seed = type_instance_of(MigrationSeedProvider::class)->assert($context->attribute('seed_provider'))->value();

        $context->client->execute(sprintf('CREATE TABLE %s (value TEXT NOT NULL)', $table));
        $context->client->execute(sprintf('INSERT INTO %s (value) VALUES ($1)', $table), [$seed]);
    }

    public function transactional(): bool
    {
        return true;
    }
};
