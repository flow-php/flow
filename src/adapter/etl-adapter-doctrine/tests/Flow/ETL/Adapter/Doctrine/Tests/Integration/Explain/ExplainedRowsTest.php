<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration\Explain;

use Flow\ETL\Adapter\Doctrine\Explain\ExplainedRows;
use Flow\ETL\Adapter\Doctrine\Tests\Context\PlannedTable;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;
use Flow\ETL\Cardinality;

final class ExplainedRowsTest extends IntegrationTestCase
{
    public function test_a_maximum_bounds_and_caps_the_estimate(): void
    {
        PlannedTable::create($this->pgsqlDatabaseContext, 'flow_explain_test', 5);

        static::assertEquals(
            new Cardinality(atMost: 3, estimate: 3, relativeError: Cardinality::DEFAULT_RELATIVE_ERROR),
            (new ExplainedRows())->of(
                $this->pgsqlDatabaseContext->connection(),
                'SELECT * FROM flow_explain_test',
                maximum: 3,
            ),
        );
    }

    public function test_an_offset_is_taken_off_the_estimate_and_never_below_zero(): void
    {
        PlannedTable::create($this->pgsqlDatabaseContext, 'flow_explain_test', 5);

        static::assertEquals(
            Cardinality::approximately(1),
            (new ExplainedRows())->of(
                $this->pgsqlDatabaseContext->connection(),
                'SELECT * FROM flow_explain_test',
                offset: 4,
            ),
        );
        static::assertEquals(
            Cardinality::approximately(0),
            (new ExplainedRows())->of(
                $this->pgsqlDatabaseContext->connection(),
                'SELECT * FROM flow_explain_test',
                offset: 9,
            ),
        );
    }

    public function test_mysql_plans_are_read(): void
    {
        PlannedTable::create($this->mysqlDatabaseContext, 'flow_explain_test', 5);

        static::assertEquals(
            Cardinality::approximately(5),
            (new ExplainedRows())->of($this->mysqlDatabaseContext->connection(), 'SELECT * FROM flow_explain_test'),
        );
    }

    public function test_sqlite_declares_only_the_maximum(): void
    {
        PlannedTable::create($this->sqliteDatabaseContext, 'flow_explain_test', 5);

        static::assertEquals(Cardinality::unknown(), (new ExplainedRows())->of(
            $this->sqliteDatabaseContext->connection(),
            'SELECT * FROM flow_explain_test',
        ));
        static::assertEquals(
            Cardinality::atMost(3),
            (new ExplainedRows())->of(
                $this->sqliteDatabaseContext->connection(),
                'SELECT * FROM flow_explain_test',
                maximum: 3,
            ),
        );
    }
}
