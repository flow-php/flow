<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\TypedCollection\ScalarType;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\ArrayUnpackTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;
use PHPUnit\Framework\TestCase;

final class ArrayExpandTransformerTest extends TestCase
{
    public function test_array_expand_collection_of_arrays() : void
    {
        $arrayExpandTransformer = Transform::array_expand('array_entry');

        $rows = $arrayExpandTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_entry', [
                        [
                            'id' => 1,
                            'status' => 'PENDING',
                            'enabled' => true,
                            'datetime' =>  new \DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                            'array' => ['foo', 'bar'],
                            'json' => '["foo", "bar"]',
                            'object' => new \stdClass(),
                            'null' => null,
                            'stringWithFloat' => '0.0',
                            'float' => 10.01,
                        ],
                        [
                            'id' => 2,
                            'status' => 'NEW',
                            'enabled' => true,
                            'datetime' =>  new \DateTimeImmutable('2020-01-05 00:00:00 UTC'),
                            'array' => ['foo', 'bar'],
                            'json' => '["foo", "bar"]',
                            'object' => new \stdClass(),
                            'null' => null,
                            'stringWithFloat' => '0.0',
                            'float' => 10.01,
                        ],
                    ]),
                ),
            ),
        );

        $this->assertEquals(
            [
                [
                    'id' => 1,
                    'status' => 'PENDING',
                    'enabled' => true,
                    'datetime' =>  new \DateTimeImmutable('2020-01-01T00:00:00+00:00'),
                    'array' => ['foo', 'bar'],
                    'json' => '["foo","bar"]',
                    'object' => new \stdClass(),
                    'null' => null,
                    'stringWithFloat' => '0.0',
                    'float' => 10.01,
                ],
                [
                    'id' => 2,
                    'status' => 'NEW',
                    'enabled' => true,
                    'datetime' =>  new \DateTimeImmutable('2020-01-05T00:00:00+00:00'),
                    'array' => ['foo', 'bar'],
                    'json' => '["foo","bar"]',
                    'object' => new \stdClass(),
                    'null' => null,
                    'stringWithFloat' => '0.0',
                    'float' => 10.01,
                ],
            ],
            (new RemoveEntriesTransformer('element'))->transform(
                (new ArrayUnpackTransformer('element'))->transform($rows)
            )->toArray()
        );
    }

    public function test_array_expand_collection_of_primitives() : void
    {
        $arrayExpandTransformer = Transform::array_expand('array_entry');

        $rows = $arrayExpandTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_entry', [1, 2]),
                ),
            ),
        );

        $this->assertEquals(
            [
                [
                    'element' => 1,
                ],
                [
                    'element' => 2,
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_array_expand_collection_of_primitives_that_keeps_other_entries() : void
    {
        $arrayExpandTransformer = Transform::array_expand('array_entry');

        $rows = $arrayExpandTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\StringEntry('string_entry', 'foo'),
                    new Row\Entry\ArrayEntry('array_entry', [1, 2]),
                ),
            ),
        );

        $this->assertEquals(
            [
                [
                    'element' => 1,
                    'string_entry' => 'foo',
                ],
                [
                    'element' => 2,
                    'string_entry' => 'foo',
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_array_expand_list_of_string() : void
    {
        $arrayExpandTransformer = Transform::array_expand('array_entry');

        $rows = $arrayExpandTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ListEntry('array_entry', ScalarType::string, ['1', '2']),
                ),
            ),
        );

        $this->assertEquals(
            [
                [
                    'element' => 1,
                ],
                [
                    'element' => 2,
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_array_expand_non_array() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('integer_entry is not ArrayEntry but Flow\ETL\Row\Entry\IntegerEntry');

        $arrayUnpackTransformer = Transform::array_expand('integer_entry');

        $arrayUnpackTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('integer_entry', 1),
                ),
            ),
        );
    }
}
