<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Fixtures\Example;
use Flow\ETL\Transformer\ObjectToArrayTransformer;
use Laminas\Hydrator\ReflectionHydrator;
use PHPUnit\Framework\TestCase;

final class ObjectToArrayTransformerTest extends TestCase
{
    public function test_object_to_array_when_entry_is_not_object() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('"integer_entry" is not ObjectEntry');

        $objectHydratorTransformer = new ObjectToArrayTransformer(
            new ReflectionHydrator(),
            'integer_entry'
        );

        $objectHydratorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('integer_entry', 1000),
                    new Row\Entry\ObjectEntry('object_entry', new Example()),
                ),
            ),
        );
    }

    public function test_object_to_array_when_entry_does_not_exists() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('"object_entry" not found');

        $objectHydratorTransformer = new ObjectToArrayTransformer(
            new ReflectionHydrator(),
            'object_entry'
        );

        $objectHydratorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('integer_entry', 1000),
                ),
            ),
        );
    }

    public function test_object_to_array_transformer() : void
    {
        $objectHydratorTransformer = new ObjectToArrayTransformer(
            new ReflectionHydrator(),
            'object_entry'
        );

        $rows = $objectHydratorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('old_int', 1000),
                    new Row\Entry\ObjectEntry('object_entry', new Example()),
                ),
            ),
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
}
