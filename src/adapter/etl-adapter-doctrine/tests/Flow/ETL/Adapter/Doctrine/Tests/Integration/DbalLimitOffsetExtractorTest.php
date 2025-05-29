<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Integration;

use function Flow\ETL\Adapter\Doctrine\from_dbal_limit_offset;
use Flow\ETL\Adapter\Doctrine\Table;
use Flow\ETL\Adapter\Doctrine\Tests\IntegrationTestCase;

final class DbalLimitOffsetExtractorTest extends IntegrationTestCase
{
    public function test_creating_limit_offset_extractor_for_table_without_oder_by() : void
    {
        $this->expectExceptionMessage('There must be at least one column to order by, zero given');

        from_dbal_limit_offset(
            $this->pgsqlDatabaseContext->connection(),
            new Table('table', ['id', 'name']),
            []
        );
    }
}
