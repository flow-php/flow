<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Tests\Double\SpyClient;
use Flow\ETL\Adapter\PostgreSql\Tests\Double\StubCursor;
use Flow\ETL\Adapter\PostgreSql\Tests\Mother\ColumnMother;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Tests\FlowTestCase;

use function extension_loaded;
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_limit_offset;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class PostgreSqlLimitOffsetExtractorTest extends FlowTestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            static::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true`',
            );
        }
    }

    public function test_a_declared_schema_runs_no_query(): void
    {
        $client = new SpyClient();

        from_pgsql_limit_offset($client, 'SELECT id FROM t ORDER BY id')
            ->withSchema(schema(int_schema('id')))
            ->schema();

        static::assertSame([], $client->calls);
    }

    public function test_a_declared_schema_wins_over_the_probe(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            from_pgsql_limit_offset(
                (new SpyClient())->willDescribe(ColumnMother::of(['other' => 'text'])),
                'SELECT id FROM t ORDER BY id',
            )
                ->withSchema(schema(int_schema('id')))
                ->schema(),
        );
    }

    public function test_extract_casts_through_the_derived_schema(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8', 'amount' => 'numeric']))
            ->willCountTotal(1)
            ->willReturnCursors(new StubCursor([['id' => '1', 'amount' => '10.5']]));

        $row = iterator_to_array(
            from_pgsql_limit_offset($client, 'SELECT id, amount FROM t ORDER BY id')->extract(flow_context()),
        )[0]->first();

        static::assertSame(1, $row->get('id'));
        static::assertSame(10.5, $row->get('amount'));
    }

    public function test_extract_derives_the_schema_once_and_reuses_it_across_batches(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willCountTotal(3)
            ->willReturnCursors(new StubCursor([['id' => '1'], ['id' => '2']]), new StubCursor([['id' => '3']]));

        $extractor = from_pgsql_limit_offset($client, 'SELECT id FROM t ORDER BY id')->withPageSize(2);
        $batches = iterator_to_array($extractor->extract(flow_context()));

        static::assertSame(1, $client->callsTo('describe'));

        foreach ($batches as $batch) {
            static::assertTrue($batch->schema()->isSame($extractor->schema()));
        }
    }

    public function test_extract_refuses_to_read_when_the_schema_cannot_be_derived(): void
    {
        $client = (new SpyClient())->willDescribe(ColumnMother::of(['location' => 'point']));

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('column "location" has PostgreSQL type "point", which Flow has no type for');

        try {
            iterator_to_array(
                from_pgsql_limit_offset($client, 'SELECT location FROM t ORDER BY location')->extract(flow_context()),
            );
        } finally {
            // The probe runs after the local ORDER BY guard and before countTotal(), so a refused
            // probe costs exactly one round trip and never reaches the counting query.
            static::assertSame(['describe'], $client->calls);
        }
    }

    public function test_schema_is_derived_from_result_metadata(): void
    {
        $schema = from_pgsql_limit_offset(
            (new SpyClient())->willDescribe(ColumnMother::of(['id' => 'int8', 'label' => 'text'])),
            'SELECT id, label FROM t ORDER BY id',
        )->schema();

        static::assertSame(['id', 'label'], $schema->references()->names());
        static::assertTrue($schema->findDefinition('id')?->isNullable());
    }

    public function test_the_derived_schema_is_memoised(): void
    {
        $client = (new SpyClient())->willDescribe(ColumnMother::of(['id' => 'int8']));
        $extractor = from_pgsql_limit_offset($client, 'SELECT id FROM t ORDER BY id');

        $extractor->schema();
        $extractor->schema();

        static::assertSame(['describe'], $client->calls);
    }

    public function test_throws_exception_when_query_has_no_order_by(): void
    {
        $this->expectExceptionMessage('LIMIT/OFFSET pagination requires ORDER BY clause for deterministic results');

        iterator_to_array(from_pgsql_limit_offset(new SpyClient(), 'SELECT id FROM t')->extract(flow_context()));
    }

    public function test_with_page_size_returns_the_same_extractor(): void
    {
        $extractor = from_pgsql_limit_offset(new SpyClient(), 'SELECT id FROM t ORDER BY id');

        static::assertSame($extractor, $extractor->withPageSize(10));
    }

    public function test_with_maximum_returns_the_same_extractor(): void
    {
        $extractor = from_pgsql_limit_offset(new SpyClient(), 'SELECT id FROM t ORDER BY id');

        static::assertSame($extractor, $extractor->withMaximum(10));
    }

    public function test_with_schema_returns_the_same_extractor(): void
    {
        $extractor = from_pgsql_limit_offset(new SpyClient(), 'SELECT id FROM t ORDER BY id');

        static::assertSame($extractor, $extractor->withSchema(schema(int_schema('id'))));
    }

    public function test_with_maximum_validates_negative_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got -1');

        from_pgsql_limit_offset(new SpyClient(), 'SELECT id FROM t ORDER BY id')->withMaximum(-1);
    }

    public function test_with_maximum_validates_positive_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got 0');

        from_pgsql_limit_offset(new SpyClient(), 'SELECT id FROM t ORDER BY id')->withMaximum(0);
    }

    public function test_with_page_size_validates_negative_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Page size must be greater than 0, got -1');

        from_pgsql_limit_offset(new SpyClient(), 'SELECT id FROM t ORDER BY id')->withPageSize(-1);
    }

    public function test_with_page_size_validates_positive_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Page size must be greater than 0, got 0');

        from_pgsql_limit_offset(new SpyClient(), 'SELECT id FROM t ORDER BY id')->withPageSize(0);
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_pgsql_limit_offset(new SpyClient(), 'SELECT id FROM t ORDER BY id')->isRepeatable());
    }
}
