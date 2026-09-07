<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Tests\Double\SpyClient;
use Flow\ETL\Adapter\PostgreSql\Tests\Double\StubCursor;
use Flow\ETL\Adapter\PostgreSql\Tests\Mother\ColumnMother;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\PostgreSql\from_pgsql_cursor;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class PostgreSqlCursorExtractorTest extends FlowTestCase
{
    public function test_a_declared_schema_runs_no_query(): void
    {
        $client = new SpyClient();

        from_pgsql_cursor($client, 'SELECT id FROM t')->withSchema(schema(int_schema('id')))->schema();

        static::assertSame([], $client->calls);
    }

    public function test_a_declared_schema_wins_over_the_probe(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            from_pgsql_cursor(
                (new SpyClient())->willDescribe(ColumnMother::of(['other' => 'text'])),
                'SELECT id FROM t',
            )
                ->withSchema(schema(int_schema('id')))
                ->schema(),
        );
    }

    public function test_cursor_loop_breaks_immediately_when_empty_result(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willReturnCursors(new StubCursor());

        static::assertSame(
            [],
            iterator_to_array(from_pgsql_cursor($client, 'SELECT id FROM t')->extract(flow_context())),
        );
        static::assertSame(1, $client->callsTo('cursor'));
    }

    public function test_cursor_loop_breaks_when_rows_less_than_fetch_size(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willReturnCursors(new StubCursor([['id' => '1'], ['id' => '2']]));

        static::assertCount(
            2,
            iterator_to_array(
                from_pgsql_cursor($client, 'SELECT id FROM t')->withFetchSize(5)->extract(flow_context()),
            ),
        );
        static::assertSame(1, $client->callsTo('cursor'));
    }

    public function test_cursor_loop_fetches_multiple_batches_when_needed(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willReturnCursors(new StubCursor([['id' => '1'], ['id' => '2']]), new StubCursor([['id' => '3']]));

        static::assertCount(
            3,
            iterator_to_array(
                from_pgsql_cursor($client, 'SELECT id FROM t')->withFetchSize(2)->extract(flow_context()),
            ),
        );
        static::assertSame(2, $client->callsTo('cursor'));
    }

    public function test_cursor_loop_with_exact_fetch_size_multiple_does_extra_fetch(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willReturnCursors(new StubCursor([['id' => '1'], ['id' => '2']]), new StubCursor());

        static::assertCount(
            2,
            iterator_to_array(
                from_pgsql_cursor($client, 'SELECT id FROM t')->withFetchSize(2)->extract(flow_context()),
            ),
        );
        static::assertSame(2, $client->callsTo('cursor'));
    }

    public function test_extract_declares_and_closes_its_own_cursor_in_its_own_transaction(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willReturnCursors(new StubCursor([['id' => '1']]));

        iterator_to_array(from_pgsql_cursor($client, 'SELECT id FROM t')->extract(flow_context()));

        // The two execute() calls are DECLARE CURSOR and the finally block's CLOSE; dropping either
        // leaks a server-side cursor inside the caller's transaction.
        static::assertSame(['describe', 'beginTransaction', 'execute', 'cursor', 'execute', 'commit'], $client->calls);
    }

    public function test_extract_joins_a_callers_transaction_instead_of_opening_its_own(): void
    {
        $client = (new SpyClient(transactionNestingLevel: 1))
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willReturnCursors(new StubCursor([['id' => '1']]));

        iterator_to_array(from_pgsql_cursor($client, 'SELECT id FROM t')->extract(flow_context()));

        // Already nested, so the extractor neither begins nor commits; the probe takes a savepoint
        // and releases it, which is the beginTransaction/rollBack pair at the front.
        static::assertSame(
            ['beginTransaction', 'describe', 'rollBack', 'execute', 'cursor', 'execute'],
            $client->calls,
        );
    }

    public function test_extract_casts_an_array_column_through_the_derived_schema(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['tags' => '_text']))
            ->willReturnCursors(new StubCursor([['tags' => ['a', 'b']]]));

        $batches = iterator_to_array(from_pgsql_cursor($client, 'SELECT tags FROM t')->extract(flow_context()));

        static::assertSame(['a', 'b'], $batches[0]->first()->get('tags'));
    }

    public function test_extract_casts_through_the_derived_schema(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8', 'amount' => 'numeric']))
            ->willReturnCursors(new StubCursor([['id' => '1', 'amount' => '10.5']]));

        $row = iterator_to_array(
            from_pgsql_cursor($client, 'SELECT id, amount FROM t')->extract(flow_context()),
        )[0]->first();

        static::assertSame(1, $row->get('id'));
        static::assertSame(10.5, $row->get('amount'));
    }

    public function test_extract_derives_the_schema_once_and_reuses_it_across_batches(): void
    {
        $client = (new SpyClient())
            ->willDescribe(ColumnMother::of(['id' => 'int8']))
            ->willReturnCursors(new StubCursor([['id' => '1'], ['id' => '2']]), new StubCursor([['id' => '3']]));

        $extractor = from_pgsql_cursor($client, 'SELECT id FROM t')->withFetchSize(2);
        $batches = iterator_to_array($extractor->extract(flow_context()));

        static::assertSame(1, $client->callsTo('describe'));

        // Never identity: the hydrator may hand back a fresh Schema per batch, so what this pins is
        // that every batch carries the same shape as the one schema() promised.
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
            iterator_to_array(from_pgsql_cursor($client, 'SELECT location FROM t')->extract(flow_context()));
        } finally {
            // The derivation happens before the transaction opens, so a refused probe leaves
            // nothing to unwind. Deriving inside the try block would fail this.
            static::assertSame(['describe'], $client->calls);
        }
    }

    public function test_schema_is_derived_from_result_metadata(): void
    {
        $schema = from_pgsql_cursor(
            (new SpyClient())->willDescribe(ColumnMother::of(['id' => 'int8', 'label' => 'text'])),
            'SELECT id, label FROM t',
        )->schema();

        static::assertSame(['id', 'label'], $schema->references()->names());
        static::assertTrue($schema->findDefinition('id')?->isNullable());
    }

    public function test_the_derived_schema_is_memoised(): void
    {
        $client = (new SpyClient())->willDescribe(ColumnMother::of(['id' => 'int8']));
        $extractor = from_pgsql_cursor($client, 'SELECT id FROM t');

        $extractor->schema();
        $extractor->schema();

        static::assertSame(['describe'], $client->calls);
    }

    public function test_with_fetch_size_validates_negative_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Fetch size must be greater than 0, got -1');

        from_pgsql_cursor(new SpyClient(), 'SELECT id FROM t')->withFetchSize(-1);
    }

    public function test_with_fetch_size_validates_positive_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Fetch size must be greater than 0, got 0');

        from_pgsql_cursor(new SpyClient(), 'SELECT id FROM t')->withFetchSize(0);
    }

    public function test_with_fetch_size_returns_the_same_extractor(): void
    {
        $extractor = from_pgsql_cursor(new SpyClient(), 'SELECT id FROM t');

        static::assertSame($extractor, $extractor->withFetchSize(10));
    }

    public function test_with_maximum_returns_the_same_extractor(): void
    {
        $extractor = from_pgsql_cursor(new SpyClient(), 'SELECT id FROM t');

        static::assertSame($extractor, $extractor->withMaximum(10));
    }

    public function test_with_cursor_name_returns_the_same_extractor(): void
    {
        $extractor = from_pgsql_cursor(new SpyClient(), 'SELECT id FROM t');

        static::assertSame($extractor, $extractor->withCursorName('c'));
    }

    public function test_with_schema_returns_the_same_extractor(): void
    {
        $extractor = from_pgsql_cursor(new SpyClient(), 'SELECT id FROM t');

        static::assertSame($extractor, $extractor->withSchema(schema(int_schema('id'))));
    }

    public function test_with_maximum_validates_negative_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got -1');

        from_pgsql_cursor(new SpyClient(), 'SELECT id FROM t')->withMaximum(-1);
    }

    public function test_with_maximum_validates_positive_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Maximum must be greater than 0, got 0');

        from_pgsql_cursor(new SpyClient(), 'SELECT id FROM t')->withMaximum(0);
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_pgsql_cursor(new SpyClient(), 'SELECT id FROM t')->isRepeatable());
    }
}
