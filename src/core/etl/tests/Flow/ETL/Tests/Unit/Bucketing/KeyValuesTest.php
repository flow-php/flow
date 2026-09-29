<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\KeyValues;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class KeyValuesTest extends FlowTestCase
{
    public function test_a_missing_ref_throws(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "id" not found');

        (new KeyValues([ref('id')]))->of(array_to_rows([['name' => 'flow']], schema(str_schema('name'))));
    }

    public function test_duplicated_refs_are_extracted_per_position(): void
    {
        static::assertSame(
            [[1, 1]],
            (new KeyValues([ref('id'), ref('id')]))->of(array_to_rows([['id' => 1]], schema(int_schema('id')))),
        );
    }

    public function test_no_refs_is_an_empty_key_per_row(): void
    {
        static::assertSame(
            [[], []],
            (new KeyValues([]))->of(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')))),
        );
    }

    public function test_of_an_empty_batch_is_empty(): void
    {
        static::assertSame([], (new KeyValues([ref('id')]))->of(rows(schema(str_schema('name')))));
    }

    public function test_of_preserves_null(): void
    {
        static::assertSame(
            [[null], ['a']],
            (new KeyValues([ref('id')]))->of(array_to_rows([
                ['id' => null],
                ['id' => 'a'],
            ], schema(str_schema('id', nullable: true)))),
        );
    }

    public function test_of_projects_each_row(): void
    {
        static::assertSame(
            [[1], [2]],
            (new KeyValues([ref('id')]))->of(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')))),
        );
    }

    public function test_of_projects_refs_in_order(): void
    {
        static::assertSame(
            [[1, 'PL'], [2, 'DE']],
            (new KeyValues([ref('id'), ref('country')]))->of(array_to_rows(
                [['id' => 1, 'country' => 'PL'], ['id' => 2, 'country' => 'DE']],
                schema(int_schema('id'), str_schema('country')),
            )),
        );
    }
}
