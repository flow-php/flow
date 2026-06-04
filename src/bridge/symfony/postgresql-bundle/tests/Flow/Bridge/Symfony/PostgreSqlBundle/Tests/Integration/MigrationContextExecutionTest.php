<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration;

use PHPUnit\Framework\TestCase;

final class MigrationContextExecutionTest extends TestCase
{
    private MigrationExecutionContext $context;

    protected function setUp(): void
    {
        $this->context = new MigrationExecutionContext();
    }

    protected function tearDown(): void
    {
        $this->context->shutdown();
    }

    public function test_migration_reads_configured_private_service_and_literal_from_context(): void
    {
        $this->context->bootConfiguredContext();

        $this->context->migrator()->migrate();

        static::assertTrue($this->context->tableExists(MigrationExecutionContext::CONFIGURED_TABLE));
        static::assertSame(
            'seeded-by-service',
            $this->context->fetchValue(MigrationExecutionContext::CONFIGURED_TABLE),
        );
    }

    public function test_migration_reads_parameter_and_service_from_service_container(): void
    {
        $this->context->bootContainerAccess();

        $this->context->migrator()->migrate();

        static::assertTrue($this->context->tableExists(MigrationExecutionContext::CONTAINER_TABLE));
        static::assertSame('seeded-by-service', $this->context->fetchValue(MigrationExecutionContext::CONTAINER_TABLE));
    }
}
