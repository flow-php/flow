<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\DropDuplicatesTransformer;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DropDuplicatesTransformerTest extends FlowTestCase
{
    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'), str_schema('name'));

        static::assertEquals($input, (new DropDuplicatesTransformer('id'))->bind($input)->output);
    }

    public function test_drop_duplicates_without_providing_entries(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DropDuplicatesTransformer requires at least one entry');

        new DropDuplicatesTransformer();
    }

    public function test_dropping_duplicated_entries_from_rows(): void
    {
        $transformer = new DropDuplicatesTransformer('id');

        $rows = array_to_rows(
            [
                ['id' => 1, 'name' => 'name1'],
                ['id' => 1, 'name' => 'name1'],
                ['id' => 2, 'name' => 'name2'],
                ['id' => 2, 'name' => 'name2'],
                ['id' => 3, 'name' => 'name3'],
            ],
            schema(int_schema('id'), str_schema('name')),
        );

        static::assertEquals(
            array_to_rows(
                [['id' => 1, 'name' => 'name1'], ['id' => 2, 'name' => 'name2'], ['id' => 3, 'name' => 'name3']],
                schema(int_schema('id'), str_schema('name')),
            ),
            $transformer->transform($rows, flow_context(config())),
        );
    }

    public function test_dropping_duplicates_when_not_all_rows_has_expected_entry(): void
    {
        $transformer = new DropDuplicatesTransformer('id');

        $rows = array_to_rows(
            [
                ['id' => 1, 'name' => 'name1'],
                ['id' => 1, 'name' => 'name1'],
                ['id' => 2, 'name' => 'name2'],
                ['id' => 2, 'name' => 'name2'],
                ['name' => 'name3'],
                ['id' => 4, 'name' => 'name4'],
            ],
            schema(int_schema('id', nullable: true), str_schema('name')),
        );

        static::assertEquals(
            array_to_rows(
                [
                    ['id' => 1, 'name' => 'name1'],
                    ['id' => 2, 'name' => 'name2'],
                    ['name' => 'name3'],
                    ['id' => 4, 'name' => 'name4'],
                ],
                schema(int_schema('id', nullable: true), str_schema('name')),
            ),
            $transformer->transform($rows, flow_context(config())),
        );
    }

    public function test_dropping_duplications_based_on_two_entries(): void
    {
        $transformer = new DropDuplicatesTransformer('id', 'name');

        $rows = array_to_rows(
            [
                ['id' => 1, 'name' => 'name1'],
                ['id' => 1, 'name' => 'name1'],
                ['id' => 2, 'name' => 'name2'],
                ['id' => 2, 'name' => 'name2'],
                ['id' => 3, 'name' => 'name3'],
            ],
            schema(int_schema('id'), str_schema('name')),
        );

        static::assertEquals(
            array_to_rows(
                [['id' => 1, 'name' => 'name1'], ['id' => 2, 'name' => 'name2'], ['id' => 3, 'name' => 'name3']],
                schema(int_schema('id'), str_schema('name')),
            ),
            $transformer->transform($rows, flow_context(config())),
        );
    }
}
