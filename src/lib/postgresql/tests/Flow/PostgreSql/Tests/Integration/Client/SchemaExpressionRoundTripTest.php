<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\Diff\ConstraintComparator;
use Flow\PostgreSql\Schema\Diff\GreedySimilarityRenameStrategy;
use Flow\PostgreSql\Schema\Diff\IndexComparator;
use Flow\PostgreSql\Schema\Diff\SimilarTextStrategy;
use Flow\PostgreSql\Schema\Diff\TableComparator;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use Flow\PostgreSql\Tests\Mother\ExpressionRoundTripTableMother;

use function extension_loaded;
use function Flow\PostgreSql\DSL\client_catalog_provider;
use function Flow\PostgreSql\DSL\create;

final class SchemaExpressionRoundTripTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_schema_expression_round_trip';

    protected function setUp(): void
    {
        parent::setUp();

        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA)->toSql());
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->function(self::SCHEMA . '.trg_noop_fn')
                    ->arguments()
                    ->returns(ColumnType::custom('trigger'))
                    ->language('plpgsql')
                    ->as('BEGIN RETURN NEW; END;')
                    ->toSql(),
            );
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);

        parent::tearDown();
    }

    public function test_collated_generated_column_round_trips(): void
    {
        $declared = ExpressionRoundTripTableMother::declared(
            self::SCHEMA,
            generationExpression: 'lower(i::text) COLLATE "C"',
        );

        foreach ($declared->toSql() as $sql) {
            $this->pgsqlContext()->client()->execute($sql);
        }

        $introspected = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table('t');

        static::assertTrue(
            (new TableComparator(
                new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())),
                new ConstraintComparator(),
                new GreedySimilarityRenameStrategy(new SimilarTextStrategy()),
            ))
                ->compare($introspected, $declared)
                ->isEmpty(),
        );
    }

    public function test_declared_table_creates_valid_ddl(): void
    {
        $sqls = ExpressionRoundTripTableMother::declared(self::SCHEMA)->toSql();

        foreach ($sqls as $sql) {
            $this->pgsqlContext()->client()->execute($sql);
        }

        static::assertCount(3, $sqls);
    }

    public function test_introspected_table_has_no_drift_against_declared(): void
    {
        $declared = ExpressionRoundTripTableMother::declared(self::SCHEMA);

        foreach ($declared->toSql() as $sql) {
            $this->pgsqlContext()->client()->execute($sql);
        }

        $introspected = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table('t');

        static::assertTrue(
            (new TableComparator(
                new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())),
                new ConstraintComparator(),
                new GreedySimilarityRenameStrategy(new SimilarTextStrategy()),
            ))
                ->compare($introspected, $declared)
                ->isEmpty(),
        );
    }

    public function test_partial_unique_index_keeps_its_predicate(): void
    {
        foreach (ExpressionRoundTripTableMother::declared(self::SCHEMA)->toSql() as $sql) {
            $this->pgsqlContext()->client()->execute($sql);
        }

        $introspected = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table('t');

        static::assertSame('deleted_at IS NULL', $introspected->indexes[0]->predicateKey());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                'INSERT INTO ' . self::SCHEMA . ".t (i, email, deleted_at) VALUES (1, 'a@x', now()), (2, 'a@x', NULL)",
            );

        $this->expectException(QueryException::class);

        $this
            ->pgsqlContext()
            ->client()
            ->execute('INSERT INTO ' . self::SCHEMA . ".t (i, email) VALUES (3, 'a@x')");
    }

    public function test_qualified_trigger_function_round_trips(): void
    {
        $declared = ExpressionRoundTripTableMother::declared(self::SCHEMA, functionName: self::SCHEMA . '.trg_noop_fn');

        foreach ($declared->toSql() as $sql) {
            $this->pgsqlContext()->client()->execute($sql);
        }

        $introspected = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table('t');

        static::assertTrue(
            (new TableComparator(
                new IndexComparator(new GreedySimilarityRenameStrategy(new SimilarTextStrategy())),
                new ConstraintComparator(),
                new GreedySimilarityRenameStrategy(new SimilarTextStrategy()),
            ))
                ->compare($introspected, $declared)
                ->isEmpty(),
        );
    }

    public function test_trigger_keeps_its_when_condition(): void
    {
        foreach (ExpressionRoundTripTableMother::declared(self::SCHEMA)->toSql() as $sql) {
            $this->pgsqlContext()->client()->execute($sql);
        }

        $introspected = client_catalog_provider($this->pgsqlContext()->client(), [self::SCHEMA])
            ->get()
            ->get(self::SCHEMA)
            ->table('t');

        static::assertSame('new.i > 0', $introspected->triggers[0]->whenConditionKey());
    }
}
