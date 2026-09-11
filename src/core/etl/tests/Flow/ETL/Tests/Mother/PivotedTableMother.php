<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\PivotedTable;
use Flow\ETL\GroupBy\PivotShape;
use Flow\ETL\Schema;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PivotedTableMother
{
    /**
     * Groups "product", pivots "country" over $values, aggregates "amount".
     */
    public static function of(AggregatingFunction $aggregate, string ...$values): PivotedTable
    {
        return self::boundTo(self::schema(), $aggregate, ...$values);
    }

    /**
     * The same table bound against a caller-supplied input schema, for cases that need "country"
     * nullable or an extra column.
     */
    public static function boundTo(Schema $input, AggregatingFunction $aggregate, string ...$values): PivotedTable
    {
        $groupBy = new GroupBy(ref('product'));
        $groupBy->pivot(ref('country'), pivot_values(...$values));
        $groupBy->aggregate($aggregate);

        return new PivotedTable($groupBy, PivotShape::of($groupBy, $input));
    }

    public static function schema(bool $nullableCountry = false): Schema
    {
        return schema(str_schema('product'), str_schema('country', nullable: $nullableCountry), int_schema('amount'));
    }
}
