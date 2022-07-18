<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Fixtures\Example;
use PHPUnit\Framework\TestCase;

final class ObjectToArrayTransformerTest extends TestCase
{
    public function test_object_to_array_transformer() : void
    {
        $objectHydratorTransformer = Transform::to_array_from_object(
            'object_entry'
        );

        $rows = $objectHydratorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('old_int', 1000),
                    new Row\Entry\ObjectEntry('object_entry', new Example()),
                ),
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('old_int', 1000),
                    new Row\Entry\ArrayEntry('object_entry', [
                        'foo' => 1,
                        'bar' => 2,
                        'baz' => 3,
                        'bad' => new \DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                    ])
                ),
            ),
            $rows
        );
    }

    public function test_object_to_array_when_entry_does_not_exists() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "object_entry" does not exist');

        $objectHydratorTransformer = Transform::to_array_from_object(
            'object_entry'
        );

        $objectHydratorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('integer_entry', 1000),
                ),
            ),
            new FlowContext(Config::default())
        );
    }

    public function test_object_to_array_when_entry_is_not_object() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('"integer_entry" is not ObjectEntry');

        $objectHydratorTransformer = Transform::to_array_from_object(
            'integer_entry'
        );

        $objectHydratorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('integer_entry', 1000),
                    new Row\Entry\ObjectEntry('object_entry', new Example()),
                ),
            ),
            new FlowContext(Config::default())
        );
    }
}
