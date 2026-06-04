<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\PostgreSqlBundle\FlowPostgreSqlBundle;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\MigrationSeedProvider;
use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\MigrationContext;
use Symfony\Component\DependencyInjection\ContainerInterface;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;

return new class implements Migration {
    public function migrate(MigrationContext $context): void
    {
        $container = type_instance_of(ContainerInterface::class)->assert($context->attribute(FlowPostgreSqlBundle::SERVICE_CONTAINER));

        $table = type_string()->assert($container->getParameter('flow_test.container_table'));
        $seed = type_instance_of(MigrationSeedProvider::class)
            ->assert($container->get('flow_test.public_seed_provider'))
            ->value();

        $context->client->execute(sprintf('CREATE TABLE %s (value TEXT NOT NULL)', $table));
        $context->client->execute(sprintf('INSERT INTO %s (value) VALUES ($1)', $table), [$seed]);
    }

    public function transactional(): bool
    {
        return true;
    }
};
