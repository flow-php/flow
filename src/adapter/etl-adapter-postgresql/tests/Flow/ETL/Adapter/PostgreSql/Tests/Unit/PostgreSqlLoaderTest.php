<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Exception\RuntimeException;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\DeleteOptions;
use Flow\ETL\Adapter\PostgreSql\LoaderOptions\UpdateOptions;
use Flow\ETL\Adapter\PostgreSql\Operation;
use Flow\ETL\Adapter\PostgreSql\PostgreSqlLoader;
use Flow\ETL\Adapter\PostgreSql\Tests\Double\SpyClient;
use Flow\ETL\Adapter\PostgreSql\Tests\Mother\WideRowsMother;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Types\ValueConverters;
use LogicException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

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

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), flow_context());
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

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), flow_context());
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

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), flow_context());
    }

    public function test_load_update_requires_update_options(): void
    {
        $client = $this->createMockClient();
        $client->expects(self::never())->method('execute');

        $loader = new PostgreSqlLoader($client, 'test_table');
        $loader->withOperation(Operation::UPDATE);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('UpdateOptions must be set for UPDATE operation');

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1])), flow_context());
    }

    public function test_a_chunked_insert_joins_a_callers_transaction(): void
    {
        $client = new SpyClient(transactionNestingLevel: 1);

        // 70 columns x 1000 rows: 936 rows fit under the cap, so the INSERT is two statements
        (new PostgreSqlLoader($client, 'wide'))->load(WideRowsMother::of(70, 1000), flow_context());

        static::assertSame(['execute', 'execute'], $client->calls);
    }

    public function test_a_failing_chunk_rolls_back_the_transaction_the_loader_opened(): void
    {
        $client = (new SpyClient())->willFailExecute(2, new LogicException('second statement failed'));

        $thrown = null;

        try {
            (new PostgreSqlLoader($client, 'wide'))->load(WideRowsMother::of(70, 1000), flow_context());
        } catch (LogicException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(LogicException::class, $thrown);
        static::assertSame(['beginTransaction', 'execute', 'execute', 'rollBack'], $client->calls);
    }

    public function test_a_chunked_insert_commits_the_transaction_the_loader_opened(): void
    {
        $client = new SpyClient();

        (new PostgreSqlLoader($client, 'wide'))->load(WideRowsMother::of(70, 1000), flow_context());

        static::assertSame(['beginTransaction', 'execute', 'execute', 'commit'], $client->calls);
    }

    public function test_a_failing_chunk_leaves_the_callers_transaction_to_the_caller(): void
    {
        $client = (new SpyClient(transactionNestingLevel: 1))->willFailExecute(
            2,
            new LogicException('second statement failed'),
        );

        $thrown = null;

        try {
            (new PostgreSqlLoader($client, 'wide'))->load(WideRowsMother::of(70, 1000), flow_context());
        } catch (LogicException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(LogicException::class, $thrown);
        static::assertSame(['execute', 'execute'], $client->calls);
    }

    public function test_an_insert_that_exactly_fits_is_one_statement_without_a_transaction(): void
    {
        $client = new SpyClient();

        // 70 columns x 936 rows = 65 520 parameters, the most one statement can carry
        (new PostgreSqlLoader($client, 'wide'))->load(WideRowsMother::of(70, 936), flow_context());

        static::assertSame(['execute'], $client->calls);
    }

    public function test_load_with_empty_rows_does_not_call_client(): void
    {
        $client = $this->createMockClient();
        $client->expects(self::never())->method('execute');

        $loader = new PostgreSqlLoader($client, 'test_table');
        $loader->load(rows(schema()), flow_context());
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
