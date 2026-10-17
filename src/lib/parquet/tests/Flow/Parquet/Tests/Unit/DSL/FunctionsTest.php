<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\DSL;

use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\ColumnRef;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Query;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_all;
use function Flow\Parquet\DSL\parquet_any;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Parquet\DSL\parquet_not;
use function Flow\Parquet\DSL\parquet_query;

final class FunctionsTest extends TestCase
{
    public function test_the_functions_build_the_nodes(): void
    {
        $a = parquet_column('a')->isNull();
        $b = parquet_column('b')->isNull();

        static::assertEquals(new ColumnRef('user.age'), parquet_column('user.age'));
        static::assertEquals(new All($a, $b), parquet_all($a, $b));
        static::assertEquals(new Any($a, $b), parquet_any($a, $b));
        static::assertEquals(new Not($a), parquet_not($a));
        static::assertEquals(new Query(), parquet_query());
    }
}
