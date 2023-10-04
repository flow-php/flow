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
}
