<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\DeleteOptions;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\UpdateOptions;
use Flow\ETL\Adapter\PostgreSql\Operation;
use Flow\ETL\Adapter\PostgreSql\PostgreSqlLoader;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Types\ValueConverters;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class PostgreSqlLoaderTest extends TestCase
{
    public function test_load_delete_requires_delete_options(): void
    {
        $client = $this->createMockClient();
        $client->expects(self::never())->method('execute');

        $loader = new PostgreSqlLoader($client, 'test_table');
        $loader->withOperation(Operation::DELETE);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('DeleteOptions must be set for DELETE operation');

        $loader->load(rows(row(int_entry('id', 1))), flow_context());
    }

    public function test_load_delete_requires_primary_keys(): void
    {
        $client = $this->createMockClient();
        $client->expects(self::never())->method('execute');

        $loader = new PostgreSqlLoader($client, 'test_table');
        $loader->withOperation(Operation::DELETE);
        $loader->withDeleteOptions(new DeleteOptions([]));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Primary keys must be specified for DELETE operation');

        $loader->load(rows(row(int_entry('id', 1))), flow_context());
    }

    public function test_load_update_requires_primary_keys(): void
    {
        $client = $this->createMockClient();
        $client->expects(self::never())->method('execute');

        $loader = new PostgreSqlLoader($client, 'test_table');
        $loader->withOperation(Operation::UPDATE);
        $loader->withUpdateOptions(new UpdateOptions([]));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Primary keys must be specified for UPDATE operation');

        $loader->load(rows(row(int_entry('id', 1))), flow_context());
    }

    public function test_load_update_requires_update_options(): void
    {
        $client = $this->createMockClient();
        $client->expects(self::never())->method('execute');

        $loader = new PostgreSqlLoader($client, 'test_table');
        $loader->withOperation(Operation::UPDATE);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('UpdateOptions must be set for UPDATE operation');

        $loader->load(rows(row(int_entry('id', 1))), flow_context());
    }

    public function test_load_with_empty_rows_does_not_call_client(): void
    {
        $client = $this->createMockClient();
        $client->expects(self::never())->method('execute');

        $loader = new PostgreSqlLoader($client, 'test_table');
        $loader->load(rows(), flow_context());
    }

    /**
     * @return Client&MockObject
     */
    private function createMockClient(): Client
    {
        $client = $this->createMock(Client::class);
        $client->method('converters')->willReturn(new ValueConverters());

        return $client;
    }
}
