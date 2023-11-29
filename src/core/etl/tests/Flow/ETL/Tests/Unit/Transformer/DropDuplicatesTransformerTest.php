<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Config;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\DropDuplicatesTransformer;
use PHPUnit\Framework\TestCase;

final class DropDuplicatesTransformerTest extends TestCase
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

        $rows = new Rows(
            Row::create(int_entry('id', 1), str_entry('name', 'name1')),
            Row::create(int_entry('id', 1), str_entry('name', 'name1')),
            Row::create(int_entry('id', 2), str_entry('name', 'name2')),
            Row::create(int_entry('id', 2), str_entry('name', 'name2')),
            Row::create(int_entry('id', 3), str_entry('name', 'name3')),
        );

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'name1')),
                Row::create(int_entry('id', 2), str_entry('name', 'name2')),
                Row::create(int_entry('id', 3), str_entry('name', 'name3')),
            ),
            $transformer->transform($rows, new FlowContext(Config::default()))
        );
    }

    public function test_dropping_duplicates_when_not_all_rows_has_expected_entry() : void
    {
        $transformer = new DropDuplicatesTransformer('id');

        $rows = new Rows(
            Row::create(int_entry('id', 1), str_entry('name', 'name1')),
            Row::create(int_entry('id', 1), str_entry('name', 'name1')),
            Row::create(int_entry('id', 2), str_entry('name', 'name2')),
            Row::create(int_entry('id', 2), str_entry('name', 'name2')),
            Row::create(str_entry('name', 'name3')),
            Row::create(int_entry('id', 4), str_entry('name', 'name4')),
        );

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'name1')),
                Row::create(int_entry('id', 2), str_entry('name', 'name2')),
                Row::create(str_entry('name', 'name3')),
                Row::create(int_entry('id', 4), str_entry('name', 'name4')),
            ),
            $transformer->transform($rows, new FlowContext(Config::default()))
        );
    }

    public function test_dropping_duplications_based_on_two_entries() : void
    {
        $transformer = new DropDuplicatesTransformer('id', 'name');

        $rows = new Rows(
            Row::create(int_entry('id', 1), str_entry('name', 'name1')),
            Row::create(int_entry('id', 1), str_entry('name', 'name1')),
            Row::create(int_entry('id', 2), str_entry('name', 'name2')),
            Row::create(int_entry('id', 2), str_entry('name', 'name2')),
            Row::create(int_entry('id', 3), str_entry('name', 'name3')),
        );

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'name1')),
                Row::create(int_entry('id', 2), str_entry('name', 'name2')),
                Row::create(int_entry('id', 3), str_entry('name', 'name3')),
            ),
            $transformer->transform($rows, new FlowContext(Config::default()))
        );
    }
}
