<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class StaticEntryTransformerTest extends TestCase
{
    public function test_adding_datetime_entry() : void
    {
        $transformer = Transform::add_datetime('datetime', new \DateTimeImmutable('2021-01-01 00:00:00'));

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('id', '1'),
            )
        ), new FlowContext(Config::default()));

        $this->assertEquals(
            [
                [
                    'id' => '1',
                    'datetime' => new \DateTimeImmutable('2021-01-01 00:00:00'),
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_adding_datetime_entry_from_string_entry() : void
    {
        $transformer = Transform::add_datetime_from_string('datetime', '2021-01-01 00:00:00');

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('id', '1'),
            )
        ), new FlowContext(Config::default()));

        $this->assertEquals(
            [
                [
                    'id' => '1',
                    'datetime' => new \DateTimeImmutable('2021-01-01 00:00:00'),
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_adding_float_entry() : void
    {
        $transformer = Transform::add_float('float', 0.5);

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('id', '1'),
            )
        ), new FlowContext(Config::default()));

        $this->assertSame(
            [
                [
                    'id' => '1',
                    'float' => 0.5,
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_adding_integer_entry() : void
    {
        $transformer = Transform::add_integer('number', 1);

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('id', '1'),
            )
        ), new FlowContext(Config::default()));

        $this->assertSame(
            [
                [
                    'id' => '1',
                    'number' => 1,
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_adding_json_entry() : void
    {
        $transformer = Transform::add_json('json', [['id' => 1], ['id' => 2]]);

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('id', '1'),
            )
        ), new FlowContext(Config::default()));

        $this->assertEquals(
            [
                [
                    'id' => '1',
                    'json' => '[{"id":1},{"id":2}]',
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_adding_json_from_string_entry() : void
    {
        $transformer = Transform::add_json_from_string('json', '[{"id":1},{"id":2}]');

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('id', '1'),
            )
        ), new FlowContext(Config::default()));

        $this->assertEquals(
            [
                [
                    'id' => '1',
                    'json' => '[{"id":1},{"id":2}]',
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_adding_json_object() : void
    {
        $transformer = Transform::add_json_object('json', ['id' => 1, 'name' => 'test']);

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('id', '1'),
            )
        ), new FlowContext(Config::default()));

        $this->assertEquals(
            [
                [
                    'id' => '1',
                    'json' => '{"id":1,"name":"test"}',
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_adding_object() : void
    {
        $transformer = Transform::add_object('object', new \ArrayObject([1, 2]));

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('id', '1'),
            )
        ), new FlowContext(Config::default()));

        $this->assertEquals(
            [
                [
                    'id' => '1',
                    'object' => new \ArrayObject([1, 2]),
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_adding_string_entry() : void
    {
        $transformer = Transform::add_string('string', 'test');

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('id', '1'),
            )
        ), new FlowContext(Config::default()));

        $this->assertSame(
            [
                [
                    'id' => '1',
                    'string' => 'test',
                ],
            ],
            $rows->toArray()
        );
    }
}
