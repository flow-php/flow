<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\DropDuplicatesTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DropDuplicatesTransformerTest extends FlowTestCase
{
    public function test_drop_duplicates_without_providing_entries(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DropDuplicatesTransformer requires at least one entry');

        new DropDuplicatesTransformer();
    }

    public function test_dropping_duplicated_entries_from_rows(): void
    {
        $transformer = new DropDuplicatesTransformer('id');

        $rows = rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'name1']),
            row(['id' => 1, 'name' => 'name1']),
            row(['id' => 2, 'name' => 'name2']),
            row(['id' => 2, 'name' => 'name2']),
            row(['id' => 3, 'name' => 'name3']),
        );

        static::assertEquals(
            rows(
                schema(int_schema('id'), str_schema('name')),
                row(['id' => 1, 'name' => 'name1']),
                row(['id' => 2, 'name' => 'name2']),
                row(['id' => 3, 'name' => 'name3']),
            ),
            $transformer->transform($rows, flow_context(config())),
        );
    }

    public function test_dropping_duplicates_when_not_all_rows_has_expected_entry(): void
    {
        $transformer = new DropDuplicatesTransformer('id');

        $rows = rows(
            schema(int_schema('id', nullable: true), str_schema('name')),
            row(['id' => 1, 'name' => 'name1']),
            row(['id' => 1, 'name' => 'name1']),
            row(['id' => 2, 'name' => 'name2']),
            row(['id' => 2, 'name' => 'name2']),
            row(['name' => 'name3']),
            row(['id' => 4, 'name' => 'name4']),
        );

        static::assertEquals(
            rows(
                schema(int_schema('id', nullable: true), str_schema('name')),
                row(['id' => 1, 'name' => 'name1']),
                row(['id' => 2, 'name' => 'name2']),
                row(['name' => 'name3']),
                row(['id' => 4, 'name' => 'name4']),
            ),
            $transformer->transform($rows, flow_context(config())),
        );
    }

    public function test_dropping_duplications_based_on_two_entries(): void
    {
        $transformer = new DropDuplicatesTransformer('id', 'name');

        $rows = rows(
            schema(int_schema('id'), str_schema('name')),
            row(['id' => 1, 'name' => 'name1']),
            row(['id' => 1, 'name' => 'name1']),
            row(['id' => 2, 'name' => 'name2']),
            row(['id' => 2, 'name' => 'name2']),
            row(['id' => 3, 'name' => 'name3']),
        );

        static::assertEquals(
            rows(
                schema(int_schema('id'), str_schema('name')),
                row(['id' => 1, 'name' => 'name1']),
                row(['id' => 2, 'name' => 'name2']),
                row(['id' => 3, 'name' => 'name3']),
            ),
            $transformer->transform($rows, flow_context(config())),
        );
    }
}
