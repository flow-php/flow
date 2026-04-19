<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client;

use Flow\PostgreSql\Client\Query;
use Flow\PostgreSql\QueryBuilder\Sql;
use PHPUnit\Framework\TestCase;

final class QueryTest extends TestCase
{
    public function test_exposes_parameters() : void
    {
        self::assertSame([1, 'x'], (new Query('SELECT $1, $2', [1, 'x']))->parameters());
    }

    public function test_exposes_sql_builder() : void
    {
        $sql = self::createStub(Sql::class);

        self::assertSame($sql, (new Query($sql))->sql());
    }

    public function test_exposes_sql_string() : void
    {
        self::assertSame('SELECT 1', (new Query('SELECT 1'))->sql());
    }

    public function test_parameters_default_to_empty_list() : void
    {
        self::assertSame([], (new Query('SELECT 1'))->parameters());
    }
}
