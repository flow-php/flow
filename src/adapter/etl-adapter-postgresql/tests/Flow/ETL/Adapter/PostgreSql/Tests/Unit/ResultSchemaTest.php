<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\ReadQuery;
use Flow\ETL\Adapter\PostgreSql\ResultSchema;
use Flow\ETL\Adapter\PostgreSql\Tests\Double\SpyClient;
use Flow\ETL\Adapter\PostgreSql\Tests\Mother\ColumnMother;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\Client\Exception\PostgreSqlError;
use Flow\PostgreSql\Client\Exception\QueryException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;

use function extension_loaded;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function sprintf;

final class ResultSchemaTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{string}>
     */
    public static function provide_mapped_postgresql_types(): Generator
    {
        foreach ([
            'int2',
            'int4',
            'int8',
            'float4',
            'float8',
            'numeric',
            'bool',
            'text',
            'varchar',
            'bpchar',
            'bytea',
            'date',
            'time',
            'timestamp',
            'timestamptz',
            'uuid',
            'json',
            'jsonb',
            'xml',
            'oid',
            'interval',
            'timetz',
            'inet',
            'cidr',
            'macaddr',
            'int4range',
            'money',
            '_int4',
            '_text',
        ] as $type) {
            yield $type => [$type];
        }
    }

    /**
     * @return Generator<string, array{string}>
     */
    public static function provide_unmapped_postgresql_types(): Generator
    {
        foreach (['record', 'point', 'line', 'lseg', 'box', 'path', 'polygon', 'circle'] as $type) {
            yield $type => [$type];
        }
    }

    /**
     * @return Generator<string, array{string}>
     */
    public static function provide_states_only_the_query_can_cause(): Generator
    {
        yield 'undefined table' => ['42P01'];
        yield 'undefined column' => ['42703'];
        yield 'undefined function' => ['42883'];
        yield 'undefined object' => ['42704'];
        yield 'insufficient privilege' => ['42501'];
    }

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            static::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true`',
            );
        }
    }

    #[DataProvider('provide_mapped_postgresql_types')]
    public function test_every_mapped_postgresql_type_becomes_a_nullable_definition(string $type): void
    {
        $schema = (new ResultSchema())->of(
            (new SpyClient())->willDescribe(ColumnMother::of(['value' => $type])),
            ReadQuery::of('SELECT value FROM t', self::class),
            [],
            self::class,
        );

        static::assertTrue($schema->findDefinition('value')?->isNullable());
    }

    #[DataProvider('provide_unmapped_postgresql_types')]
    public function test_an_unmapped_postgresql_type_is_refused(string $type): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage(sprintf(
            'column "value" has PostgreSQL type "%s", which Flow has no type for',
            $type,
        ));

        (new ResultSchema())->of(
            (new SpyClient())->willDescribe(ColumnMother::of(['value' => $type])),
            ReadQuery::of('SELECT value FROM t', self::class),
            [],
            self::class,
        );
    }

    public function test_a_multi_dimensional_array_column_is_typed_one_dimensional(): void
    {
        $schema = (new ResultSchema())->of(
            (new SpyClient())->willDescribe(ColumnMother::of(['tags' => '_int4'])),
            ReadQuery::of('SELECT tags FROM t', self::class),
            [],
            self::class,
        );

        // The element type admits null because a pg array may hold SQL NULLs.
        static::assertEquals(
            type_list(type_union(type_integer(), type_null())),
            $schema->findDefinition('tags')?->type(),
        );
    }

    public function test_a_refused_probe_becomes_a_schema_not_derivable_exception(): void
    {
        $refused = QueryException::executionFailed(
            'SELECT 1;;',
            PostgreSqlError::unknown('syntax error at or near ";"'),
        );

        try {
            (new ResultSchema())->of(
                (new SpyClient())->willRefuseDescribe($refused),
                ReadQuery::of('SELECT 1;;', self::class),
                [],
                self::class,
            );
            static::fail('a refused probe must not describe');
        } catch (SchemaNotDerivableException $e) {
            static::assertSame($refused, $e->getPrevious());
            static::assertStringContainsString(
                'PostgreSQL refused the zero-row probe of this query (Query execution failed',
                $e->getMessage(),
            );
            static::assertStringContainsString(
                'If the query runs as written, declare the schema with ->withSchema() to skip the probe.',
                $e->getMessage(),
            );
        }
    }

    public function test_a_rethrown_database_error_inside_an_open_transaction_rolls_back_to_the_savepoint(): void
    {
        $client = (new SpyClient(
            transactionNestingLevel: 1,
        ))->willRefuseDescribe(QueryException::executionFailed('SELECT id FROM t', PostgreSqlError::fromDiagnostics(
            '42P01',
            'relation "t" does not exist',
        )));

        try {
            (new ResultSchema())->of($client, ReadQuery::of('SELECT id FROM t', self::class), [], self::class);
        } catch (QueryException) {
        }

        static::assertSame(['beginTransaction', 'describe', 'rollBack'], $client->calls);
    }

    #[DataProvider('provide_states_only_the_query_can_cause')]
    public function test_a_query_naming_a_missing_object_rethrows_the_database_error(string $state): void
    {
        $refused = QueryException::executionFailed('SELECT id FROM t', PostgreSqlError::fromDiagnostics($state, 'x'));

        try {
            (new ResultSchema())->of(
                (new SpyClient())->willRefuseDescribe($refused),
                ReadQuery::of('SELECT id FROM t', self::class),
                [],
                self::class,
            );
            static::fail('a refused probe must not describe');
        } catch (QueryException $e) {
            static::assertSame($refused, $e);
        }
    }

    public function test_a_refused_probe_inside_an_open_transaction_rolls_back_to_the_savepoint(): void
    {
        $client = (new SpyClient(transactionNestingLevel: 1))->willRefuseDescribe(QueryException::executionFailed(
            'SELECT 1;;',
            PostgreSqlError::unknown('syntax error'),
        ));

        try {
            (new ResultSchema())->of($client, ReadQuery::of('SELECT 1;;', self::class), [], self::class);
        } catch (SchemaNotDerivableException) {
        }

        static::assertSame(['beginTransaction', 'describe', 'rollBack'], $client->calls);
    }

    public function test_a_savepoint_wraps_the_probe_inside_an_open_transaction(): void
    {
        $nested = (new SpyClient(transactionNestingLevel: 1))->willDescribe(ColumnMother::of(['id' => 'int8']));
        (new ResultSchema())->of($nested, ReadQuery::of('SELECT id FROM t', self::class), [], self::class);

        // Rolled back, not committed: the probe is a read-only LIMIT 0, so discarding the savepoint
        // is equivalent and closes every failure path with one unconditional finally.
        static::assertSame(['beginTransaction', 'describe', 'rollBack'], $nested->calls);

        $topLevel = (new SpyClient())->willDescribe(ColumnMother::of(['id' => 'int8']));
        (new ResultSchema())->of($topLevel, ReadQuery::of('SELECT id FROM t', self::class), [], self::class);

        static::assertSame(['describe'], $topLevel->calls);
    }

    public function test_a_savepoint_is_released_when_describe_throws_something_other_than_a_query_exception(): void
    {
        // column_type_from_string() runs a SQL parser, so a pg type whose name is a reserved word
        // throws ParserException - a sibling of QueryException, not a subclass. Leaving the savepoint
        // open would desynchronise the caller's own begin/commit pairing for the rest of the session.
        $client = (new SpyClient(transactionNestingLevel: 1))->willThrowFromDescribe(
            new RuntimeException('parser blew up'),
        );

        try {
            (new ResultSchema())->of($client, ReadQuery::of('SELECT id FROM t', self::class), [], self::class);
            static::fail('the throwable should propagate');
        } catch (RuntimeException $e) {
            static::assertSame('parser blew up', $e->getMessage());
        }

        static::assertSame(['beginTransaction', 'describe', 'rollBack'], $client->calls);
    }

    public function test_duplicate_output_names_collapse_last_wins(): void
    {
        $schema = (new ResultSchema())->of(
            (new SpyClient())->willDescribe([ColumnMother::pair('a', 'int8'), ColumnMother::pair('a', 'text')]),
            ReadQuery::of('SELECT id AS a, label AS a FROM t', self::class),
            [],
            self::class,
        );

        static::assertSame(['a'], $schema->references()->names());
        static::assertEquals(type_string(), $schema->findDefinition('a')?->type());
    }

    public function test_the_probe_never_receives_the_callers_values(): void
    {
        $client = (new SpyClient())->willDescribe(ColumnMother::of(['id' => 'int8']));

        (new ResultSchema())->of(
            $client,
            ReadQuery::of('SELECT id FROM t WHERE id > $1', self::class),
            [42],
            self::class,
        );

        // ResultSchema hands the parameters straight through; PgSqlClient::describe() is what
        // substitutes nulls, so the adapter only has to prove it does not rewrite them.
        static::assertSame([[42]], $client->describeParameters);
    }
}
