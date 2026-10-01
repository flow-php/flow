<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Comparator;

use Flow\ETL\Tests\Comparator\ColumnComparator;
use Flow\ETL\Tests\Comparator\RowsComparator;
use PHPUnit\Framework\TestCase;
use SebastianBergmann\Comparator\Factory;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ComparatorExtensionTest extends TestCase
{
    public function test_rows_and_columns_compare_through_flow_comparators(): void
    {
        $rows = array_to_rows([['id' => 1]], schema(int_schema('id')));

        static::assertInstanceOf(RowsComparator::class, Factory::getInstance()->getComparatorFor($rows, $rows));
        static::assertInstanceOf(ColumnComparator::class, Factory::getInstance()->getComparatorFor(
            $rows->column('id'),
            $rows->column('id'),
        ));
    }
}
