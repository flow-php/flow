<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\Explain;

use Flow\ETL\Adapter\Doctrine\Explain\PostgreSqlExplainedRows;
use Flow\ETL\Adapter\Doctrine\Tests\Context\PlannedTable;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;

final class PostgreSqlExplainedRowsTest extends IntegrationTestCase
{
    public function test_a_parameterised_query_is_planned(): void
    {
        PlannedTable::create($this->pgsqlDatabaseContext, 'flow_explain_test', 100);

        static::assertSame(10, (new PostgreSqlExplainedRows())->of(
            $this->pgsqlDatabaseContext->connection(),
            'SELECT * FROM flow_explain_test WHERE id > ?',
            [90],
        ));
    }

    public function test_the_root_plan_rows_are_the_estimate(): void
    {
        PlannedTable::create($this->pgsqlDatabaseContext, 'flow_explain_test', 5);

        static::assertSame(5, (new PostgreSqlExplainedRows())->of(
            $this->pgsqlDatabaseContext->connection(),
            'SELECT * FROM flow_explain_test',
        ));
    }
}
