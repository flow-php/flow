<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\Tests\Integration\Context\DescribeProbeContext;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function array_column;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;

final class PgSqlClientDescribeTest extends PostgreSqlTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        DescribeProbeContext::createTable($this->pgsqlContext()->client(), 'describe_probe');
    }

    public function test_describe_binds_null_at_every_parameter_position(): void
    {
        $sql = 'SELECT id, label FROM describe_probe WHERE id > $1 AND label = $2';

        static::assertSame(
            ['id', 'label'],
            array_column($this->pgsqlContext()->client()->describe($sql, [42, 'x']), 'name'),
        );
        static::assertSame(
            ['id', 'label'],
            array_column($this->pgsqlContext()->client()->describe($sql, [null, null]), 'name'),
        );

        // The proof the substitution really happens: PostgreSQL rejects this value at the int
        // position when it is bound, so an answer here can only mean describe() bound null instead.
        static::assertSame(
            ['id', 'label'],
            array_column($this->pgsqlContext()->client()->describe($sql, ['not-an-int', 'x']), 'name'),
        );
    }

    public function test_describe_reports_columns_of_a_zero_row_result(): void
    {
        $columns = $this
            ->pgsqlContext()
            ->client()
            ->describe(select(star())->from(table('describe_probe')));

        static::assertSame(
            ['id', 'label', 'amount', 'flag', 'day', 'clock', 'moment', 'identifier', 'document', 'markup'],
            array_column($columns, 'name'),
        );
        static::assertSame(
            ['int8', 'text', 'numeric', 'bool', 'date', 'time', 'timestamptz', 'uuid', 'jsonb', 'xml'],
            array_map(static fn(array $column): string => $column['type']->normalize()['name'], $columns),
        );
    }

    public function test_describe_reports_duplicate_output_names_once_per_projection(): void
    {
        $columns = $this
            ->pgsqlContext()
            ->client()
            ->describe(select(col('id')->as('a'), col('label')->as('a'))->from(table('describe_probe')));

        static::assertSame(['a', 'a'], array_column($columns, 'name'));
        static::assertSame(
            ['int8', 'text'],
            array_map(static fn(array $column): string => $column['type']->normalize()['name'], $columns),
        );
    }

    public function test_describe_reports_the_result_of_a_computed_projection(): void
    {
        // The case a catalog route cannot answer: an alias and an expression have no catalog column.
        $columns = $this
            ->pgsqlContext()
            ->client()
            ->describe('SELECT id, label AS bb, amount * 2 AS calc FROM describe_probe');

        static::assertSame(['id', 'bb', 'calc'], array_column($columns, 'name'));
        static::assertSame(
            ['int8', 'text', 'numeric'],
            array_map(static fn(array $column): string => $column['type']->normalize()['name'], $columns),
        );
    }

    public function test_describe_wraps_a_query_that_ends_in_a_semicolon(): void
    {
        static::assertSame(
            ['id'],
            array_column($this->pgsqlContext()->client()->describe('SELECT id FROM describe_probe;'), 'name'),
        );
    }
}
