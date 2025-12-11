<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder;

use Flow\PostgreSql\Tests\Integration\QueryBuilder\Assertions\QueryBuilderAssertions;
use PHPUnit\Framework\TestCase;

abstract class PGQueryTestCase extends TestCase
{
    use QueryBuilderAssertions;

    protected function setUp() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            static::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }
    }
}
