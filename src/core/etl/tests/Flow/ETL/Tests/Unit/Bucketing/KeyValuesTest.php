<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\KeyValues;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class KeyValuesTest extends FlowTestCase
{
    public function test_of_projects_each_row(): void
    {
        static::assertSame(
            [[1], [2]],
            (new KeyValues([ref('id')]))->of(rows(row(int_entry('id', 1)), row(int_entry('id', 2)))),
        );
    }

    public function test_of_row_preserves_null(): void
    {
        static::assertSame([null], (new KeyValues([ref('id')]))->ofRow(row(int_entry('id', null))));
    }

    public function test_of_row_projects_refs_in_order(): void
    {
        static::assertSame(
            [1, 'PL'],
            (new KeyValues([ref('id'), ref('country')]))->ofRow(row(int_entry('id', 1), str_entry('country', 'PL'))),
        );
    }

    public function test_duplicated_refs_are_extracted_per_position(): void
    {
        static::assertSame([1, 1], (new KeyValues([ref('id'), ref('id')]))->ofRow(row(int_entry('id', 1))));
    }

    public function test_missing_column_throws_by_default(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new KeyValues([ref('id')]))->ofRow(row(str_entry('name', 'flow')));
    }

    public function test_null_on_missing_extracts_missing_column_as_null(): void
    {
        static::assertSame(
            [null, 'flow'],
            (new KeyValues([ref('id'), ref('name')], nullOnMissing: true))->ofRow(row(str_entry('name', 'flow'))),
        );
    }
}
