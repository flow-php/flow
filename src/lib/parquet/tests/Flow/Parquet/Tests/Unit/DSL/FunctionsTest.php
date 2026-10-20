<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\DSL;

use Flow\Parquet\Filter\ColumnRef;
use Flow\Parquet\Query;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_query;

final class FunctionsTest extends TestCase
{
    public function test_the_functions_build_the_nodes(): void
    {
        static::assertEquals(new ColumnRef('user.age'), parquet_column('user.age'));
        static::assertEquals(new Query(), parquet_query());
    }
}
