<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\Explain;

use Flow\ETL\Adapter\Doctrine\Explain\MySqlExplainedRows;
use Flow\ETL\Adapter\Doctrine\Tests\Context\PlannedTable;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;

final class MySqlExplainedRowsTest extends IntegrationTestCase
{
    public function test_a_group_by_read_back_from_a_temporary_table_is_unknown(): void
    {
        PlannedTable::create($this->mysqlDatabaseContext, 'flow_explain_test', 5);

        static::assertNull((new MySqlExplainedRows())->of(
            $this->mysqlDatabaseContext->connection(),
            'SELECT grp, COUNT(*) FROM flow_explain_test GROUP BY grp',
        ));
    }

    public function test_a_parameterised_query_is_planned(): void
    {
        PlannedTable::create($this->mysqlDatabaseContext, 'flow_explain_test', 100);

        static::assertSame(10, (new MySqlExplainedRows())->of(
            $this->mysqlDatabaseContext->connection(),
            'SELECT * FROM flow_explain_test WHERE id > ?',
            [90],
        ));
    }

    public function test_the_root_line_rows_are_the_estimate(): void
    {
        PlannedTable::create($this->mysqlDatabaseContext, 'flow_explain_test', 5);

        static::assertSame(5, (new MySqlExplainedRows())->of(
            $this->mysqlDatabaseContext->connection(),
            'SELECT * FROM flow_explain_test',
        ));
    }
}
