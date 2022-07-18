<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Config;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ArrayCollectionGetTransformerTest extends TestCase
{
    public function test_for_not_array_entry() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('invalid_entry is not ArrayEntry but Flow\ETL\Row\Entry\IntegerEntry');

        $arrayUnpackTransformer = Transform::array_collection_get(['id'], 'invalid_entry', 'new_entry');

        $arrayUnpackTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('invalid_entry', 1),
                ),
            ),
            new FlowContext(Config::default())
        );
    }

    public function test_getting_keys_from_simple_array() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('array_entry, must be an array of array (collection of arrays) but it seems to be a regular array.');

        $arrayAccessorTransformer = Transform::array_collection_get(['id', 'status'], 'array_entry');

        $rows = $arrayAccessorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry(
                        'array_entry',
                        [
                            'id' => 1,
                            'status' => 'PENDING',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ]
                    ),
                ),
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(
            [
                ['id' => 1, 'status' => 'PENDING'],
                ['id' => 2, 'status' => 'NEW'],
            ],
            $rows->first()->valueOf('element')
        );
    }

    public function test_getting_specific_keys_from_collection_of_array() : void
    {
        $arrayAccessorTransformer = Transform::array_collection_get(['id', 'status'], 'array_entry');

        $rows = $arrayAccessorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_entry', [
                        [
                            'id' => 1,
                            'status' => 'PENDING',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                        [
                            'id' => 2,
                            'status' => 'NEW',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                    ]),
                ),
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(
            [
                ['id' => 1, 'status' => 'PENDING'],
                ['id' => 2, 'status' => 'NEW'],
            ],
            $rows->first()->valueOf('element')
        );
    }

    public function test_getting_specific_keys_from_first_element_in_collection_of_array() : void
    {
        $arrayAccessorTransformer = Transform::array_collection_get_first(['parent_id'], 'array_entry');

        $rows = $arrayAccessorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_entry', [
                        [
                            'parent_id' => 1,
                            'id' => 1,
                            'status' => 'PENDING',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                        [
                            'parent_id' => 1,
                            'id' => 2,
                            'status' => 'NEW',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                    ]),
                ),
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(
            [
                'parent_id' => 1,
            ],
            $rows->first()->valueOf('element')
        );
    }

    public function test_getting_specific_keys_from_first_element_in_collection_of_array_when_first_index_does_not_exists() : void
    {
        $arrayAccessorTransformer = Transform::array_collection_get_first(['parent_id'], 'array_entry');

        $rows = $arrayAccessorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_entry', [
                        2 => [
                            'parent_id' => 1,
                            'id' => 1,
                            'status' => 'PENDING',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                        3 => [
                            'parent_id' => 1,
                            'id' => 2,
                            'status' => 'NEW',
                            'enabled' => true,
                            'array' => ['foo' => 'bar'],
                        ],
                    ]),
                ),
            ),
            new FlowContext(Config::default())
        );

        $this->assertEquals(
            [
                'parent_id' => 1,
            ],
            $rows->first()->valueOf('element')
        );
    }
}
