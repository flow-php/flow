<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{config, flow_context, row, rows};
use function Flow\ETL\DSL\{int_entry, str_entry};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Transformer\DropDuplicatesTransformer;
use Flow\ETL\{Tests\FlowTestCase};

final class DropDuplicatesTransformerTest extends FlowTestCase
{
    public function test_drop_duplicates_without_providing_entries() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DropDuplicatesTransformer requires at least one entry');

        new DropDuplicatesTransformer();
    }

    public function test_dropping_duplicated_entries_from_rows() : void
    {
        $transformer = new DropDuplicatesTransformer('id');

        $rows = rows(row(int_entry('id', 1), str_entry('name', 'name1')), row(int_entry('id', 1), str_entry('name', 'name1')), row(int_entry('id', 2), str_entry('name', 'name2')), row(int_entry('id', 2), str_entry('name', 'name2')), row(int_entry('id', 3), str_entry('name', 'name3')));

        self::assertEquals(
            rows(row(int_entry('id', 1), str_entry('name', 'name1')), row(int_entry('id', 2), str_entry('name', 'name2')), row(int_entry('id', 3), str_entry('name', 'name3'))),
            $transformer->transform($rows, flow_context(config()))
        );
    }

    public function test_dropping_duplicates_when_not_all_rows_has_expected_entry() : void
    {
        $transformer = new DropDuplicatesTransformer('id');

        $rows = rows(row(int_entry('id', 1), str_entry('name', 'name1')), row(int_entry('id', 1), str_entry('name', 'name1')), row(int_entry('id', 2), str_entry('name', 'name2')), row(int_entry('id', 2), str_entry('name', 'name2')), row(str_entry('name', 'name3')), row(int_entry('id', 4), str_entry('name', 'name4')));

        self::assertEquals(
            rows(row(int_entry('id', 1), str_entry('name', 'name1')), row(int_entry('id', 2), str_entry('name', 'name2')), row(str_entry('name', 'name3')), row(int_entry('id', 4), str_entry('name', 'name4'))),
            $transformer->transform($rows, flow_context(config()))
        );
    }

    public function test_dropping_duplications_based_on_two_entries() : void
    {
        $transformer = new DropDuplicatesTransformer('id', 'name');

        $rows = rows(row(int_entry('id', 1), str_entry('name', 'name1')), row(int_entry('id', 1), str_entry('name', 'name1')), row(int_entry('id', 2), str_entry('name', 'name2')), row(int_entry('id', 2), str_entry('name', 'name2')), row(int_entry('id', 3), str_entry('name', 'name3')));

        self::assertEquals(
            rows(row(int_entry('id', 1), str_entry('name', 'name1')), row(int_entry('id', 2), str_entry('name', 'name2')), row(int_entry('id', 3), str_entry('name', 'name3'))),
            $transformer->transform($rows, flow_context(config()))
        );
    }
}
