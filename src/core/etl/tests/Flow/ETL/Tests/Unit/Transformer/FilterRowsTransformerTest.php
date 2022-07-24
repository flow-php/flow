<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class FilterRowsTransformerTest extends TestCase
{
    public function test_entry_exists_in_rows() : void
    {
        $filterRows = Transform::filter_exists('number');

        $rows = $filterRows->transform(
            new Rows(
                Row::create(new Row\Entry\IntegerEntry('number', 2), new Row\Entry\StringEntry('text', 'test')),
                Row::create(new Row\Entry\StringEntry('text', 'test')),
            ),
            new FlowContext(Config::default())
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
        $filterRows = Transform::filter_not_exists('number');

        $rows = $filterRows->transform(
            new Rows(
                Row::create(new Row\Entry\IntegerEntry('number', 2), new Row\Entry\StringEntry('text', 'test')),
                Row::create(new Row\Entry\StringEntry('text', 'test')),
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(
            [
                ['text' => 'test'],
            ],
            $rows->toArray()
        );
    }

    public function test_filter_null_rows() : void
    {
        $filterRows = Transform::filter_not_null('number');

        $rows = $filterRows->transform(
            new Rows(
                Row::create(new Row\Entry\IntegerEntry('number', 2), new Row\Entry\StringEntry('text', 'test')),
                Row::create(new Row\Entry\NullEntry('number'), new Row\Entry\StringEntry('text', 'test')),
                Row::create(new Row\Entry\IntegerEntry('number', 5), new Row\Entry\StringEntry('text', 'test')),
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(
            [
                ['number' => 2, 'text' => 'test'],
                ['number' => 5, 'text' => 'test'],
            ],
            $rows->toArray()
        );
    }

    public function test_filter_numeric_rows() : void
    {
        $filterRows = Transform::filter_equals('number', 5);

        $rows = $filterRows->transform(
            new Rows(
                Row::create(new Row\Entry\IntegerEntry('number', 2)),
                Row::create(new Row\Entry\IntegerEntry('number', 10)),
                Row::create(new Row\Entry\IntegerEntry('number', 5)),
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(
            [
                ['number' => 5],
            ],
            $rows->toArray()
        );
    }

    public function test_filter_string_rows() : void
    {
        $filterRows = Transform::filter_equals('status', 'NEW');

        $rows = $filterRows->transform(
            new Rows(
                Row::create(new Row\Entry\StringEntry('status', 'PENDING')),
                Row::create(new Row\Entry\StringEntry('status', 'SHIPPED')),
                Row::create(new Row\Entry\StringEntry('status', 'NEW')),
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(
            [
                ['status' => 'NEW'],
            ],
            $rows->toArray()
        );
    }
}
