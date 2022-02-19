<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\Filter\Filter\EntryEqualsTo;
use Flow\ETL\Transformer\Filter\Filter\EntryExists;
use Flow\ETL\Transformer\Filter\Filter\EntryNotNull;
use Flow\ETL\Transformer\Filter\Filter\Opposite;
use Flow\ETL\Transformer\FilterRowsTransformer;
use PHPUnit\Framework\TestCase;

final class FilterRowsTransformerTest extends TestCase
{
    public function test_filter_string_rows() : void
    {
        $filterRows = new FilterRowsTransformer(
            new EntryEqualsTo('status', 'NEW'),
        );

        $rows = $filterRows->transform(
            new Rows(
                Row::create(new Row\Entry\StringEntry('status', 'PENDING')),
                Row::create(new Row\Entry\StringEntry('status', 'SHIPPED')),
                Row::create(new Row\Entry\StringEntry('status', 'NEW')),
            )
        );

        $this->assertEquals(
            [
                ['status' => 'NEW'],
            ],
            $rows->toArray()
        );
    }

    public function test_filter_numeric_rows() : void
    {
        $filterRows = new FilterRowsTransformer(
            new EntryEqualsTo('number', 5),
        );

        $rows = $filterRows->transform(
            new Rows(
                Row::create(new Row\Entry\IntegerEntry('number', 2)),
                Row::create(new Row\Entry\IntegerEntry('number', 10)),
                Row::create(new Row\Entry\IntegerEntry('number', 5)),
            )
        );

        $this->assertEquals(
            [
                ['number' => 5],
            ],
            $rows->toArray()
        );
    }

    public function test_filter_null_rows() : void
    {
        $filterRows = new FilterRowsTransformer(
            new EntryNotNull('number'),
        );

        $rows = $filterRows->transform(
            new Rows(
                Row::create(new Row\Entry\IntegerEntry('number', 2), new Row\Entry\StringEntry('text', 'test')),
                Row::create(new Row\Entry\NullEntry('number'), new Row\Entry\StringEntry('text', 'test')),
                Row::create(new Row\Entry\IntegerEntry('number', 5), new Row\Entry\StringEntry('text', 'test')),
            )
        );

        $this->assertEquals(
            [
                ['number' => 2, 'text' => 'test'],
                ['number' => 5, 'text' => 'test'],
            ],
            $rows->toArray()
        );
    }

    public function test_entry_exists_in_rows() : void
    {
        $filterRows = new FilterRowsTransformer(
            new EntryExists('number'),
        );

        $rows = $filterRows->transform(
            new Rows(
                Row::create(new Row\Entry\IntegerEntry('number', 2), new Row\Entry\StringEntry('text', 'test')),
                Row::create(new Row\Entry\StringEntry('text', 'test')),
            )
        );

        $this->assertEquals(
            [
                ['number' => 2, 'text' => 'test'],
            ],
            $rows->toArray()
        );
    }

    public function test_entry_not_exists_in_rows() : void
    {
        $filterRows = new FilterRowsTransformer(
            new Opposite(new EntryExists('number')),
        );

        $rows = $filterRows->transform(
            new Rows(
                Row::create(new Row\Entry\IntegerEntry('number', 2), new Row\Entry\StringEntry('text', 'test')),
                Row::create(new Row\Entry\StringEntry('text', 'test')),
            )
        );

        $this->assertEquals(
            [
                ['text' => 'test'],
            ],
            $rows->toArray()
        );
    }
}
