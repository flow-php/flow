<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\KeyValues;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class KeyValuesTest extends FlowTestCase
{
    public function test_of_projects_each_row(): void
    {
        static::assertSame(
            [[1], [2]],
            (new KeyValues([ref('id')]))->of(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]))),
        );
    }

    public function test_of_row_preserves_null(): void
    {
        static::assertSame([null], (new KeyValues([ref('id')]))->ofRow(row(['id' => null])));
    }

    public function test_of_row_projects_refs_in_order(): void
    {
        static::assertSame(
            [1, 'PL'],
            (new KeyValues([ref('id'), ref('country')]))->ofRow(row(['id' => 1, 'country' => 'PL'])),
        );
    }

    public function test_duplicated_refs_are_extracted_per_position(): void
    {
        static::assertSame([1, 1], (new KeyValues([ref('id'), ref('id')]))->ofRow(row(['id' => 1])));
    }

    public function test_missing_column_throws_by_default(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new KeyValues([ref('id')]))->ofRow(row(['name' => 'flow']));
    }

    public function test_null_on_missing_extracts_missing_column_as_null(): void
    {
        static::assertSame(
            [null, 'flow'],
            (new KeyValues([ref('id'), ref('name')], nullOnMissing: true))->ofRow(row(['name' => 'flow'])),
        );
    }
}
