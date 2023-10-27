<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
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
            Row::create(Entry::int('id', 1), Entry::str('name', 'name1')),
            Row::create(Entry::int('id', 1), Entry::str('name', 'name1')),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name2')),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name2')),
            Row::create(Entry::int('id', 3), Entry::str('name', 'name3')),
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::int('id', 1), Entry::str('name', 'name1')),
                Row::create(Entry::int('id', 2), Entry::str('name', 'name2')),
                Row::create(Entry::int('id', 3), Entry::string('name', 'name3')),
            ),
            $transformer->transform($rows, new FlowContext(Config::default()))
        );
    }

    public function test_dropping_duplicates_when_not_all_rows_has_expected_entry() : void
    {
        $transformer = new DropDuplicatesTransformer('id');

        $rows = new Rows(
            Row::create(Entry::int('id', 1), Entry::str('name', 'name1')),
            Row::create(Entry::int('id', 1), Entry::str('name', 'name1')),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name2')),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name2')),
            Row::create(Entry::str('name', 'name3')),
            Row::create(Entry::int('id', 4), Entry::str('name', 'name4')),
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::int('id', 1), Entry::str('name', 'name1')),
                Row::create(Entry::int('id', 2), Entry::str('name', 'name2')),
                Row::create(Entry::str('name', 'name3')),
                Row::create(Entry::int('id', 4), Entry::str('name', 'name4')),
            ),
            $transformer->transform($rows, new FlowContext(Config::default()))
        );
    }

    public function test_dropping_duplications_based_on_two_entries() : void
    {
        $transformer = new DropDuplicatesTransformer('id', 'name');

        $rows = new Rows(
            Row::create(Entry::int('id', 1), Entry::str('name', 'name1')),
            Row::create(Entry::int('id', 1), Entry::str('name', 'name1')),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name2')),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name2')),
            Row::create(Entry::int('id', 3), Entry::str('name', 'name3')),
        );

        $this->assertEquals(
            new Rows(
                Row::create(Entry::int('id', 1), Entry::str('name', 'name1')),
                Row::create(Entry::int('id', 2), Entry::str('name', 'name2')),
                Row::create(Entry::int('id', 3), Entry::str('name', 'name3')),
            ),
            $transformer->transform($rows, new FlowContext(Config::default()))
        );
    }
}
