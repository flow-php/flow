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
use function Flow\ETL\DSL\str_schema;

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
        static::assertSame(
            [null],
            (new KeyValues([ref('id')]))->ofRow(row(['id' => null]), schema(int_schema('id', nullable: true))),
        );
    }

    public function test_of_row_projects_refs_in_order(): void
    {
        static::assertSame(
            [1, 'PL'],
            (new KeyValues([ref('id'), ref('country')]))->ofRow(
                row(['id' => 1, 'country' => 'PL']),
                schema(int_schema('id'), str_schema('country')),
            ),
        );
    }

    public function test_duplicated_refs_are_extracted_per_position(): void
    {
        static::assertSame(
            [1, 1],
            (new KeyValues([ref('id'), ref('id')]))->ofRow(row(['id' => 1]), schema(int_schema('id'))),
        );
    }

    public function test_a_missing_not_null_ref_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "id" does not exist');

        (new KeyValues([ref('id')]))->ofRow(row(['name' => 'flow']), schema(int_schema('id'), str_schema('name')));
    }

    public function test_a_missing_nullable_ref_is_null_when_the_schema_allows_it(): void
    {
        static::assertSame(
            [null, 'flow'],
            (new KeyValues([ref('id'), ref('name')]))->ofRow(
                row(['name' => 'flow']),
                schema(int_schema('id', nullable: true), str_schema('name')),
            ),
        );
    }
}
