<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ObjectMethodTransformerTest extends TestCase
{
    public function test_transformer_for_missing_entry() : void
    {
        $transformer = Transform::object_method('not_existing', 'method');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('"not_existing" entry not found');

        $transformer->transform(new Rows(
            Row::create(new Row\Entry\StringEntry('name', 'test'))
        ));
    }

    public function test_transformer_for_non_object() : void
    {
        $transformer = Transform::object_method('non_object', 'method');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('"non_object" entry is not ObjectEntry');

        $transformer->transform(new Rows(
            Row::create(new Row\Entry\StringEntry('non_object', 'test'))
        ));
    }

    public function test_transformer_for_object() : void
    {
        $transformer = Transform::object_method('object', 'toArray');

        $rows = $transformer->transform(new Rows(
            Row::create(new Row\Entry\ObjectEntry('object', $object = new class {
                public function toArray() : array
                {
                    return [
                        'id' => 1,
                        'name' => 'object',
                    ];
                }
            }))
        ));

        $this->assertSame(
            [
                [
                    'object' => $object,
                    'method_entry' => [
                        'id' => 1,
                        'name' => 'object',
                    ],
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_transformer_for_object_without_expected_method() : void
    {
        $transformer = Transform::object_method('object', 'method');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('"object" is object does not have "method" method.');

        $transformer->transform(new Rows(
            Row::create(new Row\Entry\ObjectEntry('object', new \stdClass()))
        ));
    }
}
