<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DSL\Transform;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ArrayCollectionMergeTransformerTest extends TestCase
{
    public function test_attempt_of_merging_collection_where_not_every_element_is_array() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('array_entry, must be an array of arrays, instead element at position "1" is integer');

        $arrayAccessorTransformer = Transform::array_collection_merge('array_entry');

        $rows = $arrayAccessorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry(
                        'array_entry',
                        [
                            ['foo' => 'bar'],
                            1,
                        ]
                    ),
                ),
            ),
        );
    }

    public function test_for_not_array_entry() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('invalid_entry is not ArrayEntry but Flow\ETL\Row\Entry\IntegerEntry');

        $arrayUnpackTransformer = Transform::array_collection_merge('invalid_entry', 'new_entry');

        $arrayUnpackTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\IntegerEntry('invalid_entry', 1),
                ),
            ),
        );
    }

    public function test_merging_collection_of_arrays() : void
    {
        $arrayAccessorTransformer = Transform::array_collection_merge('array_entry');

        $rows = $arrayAccessorTransformer->transform(
            new Rows(
                Row::create(
                    new Row\Entry\ArrayEntry('array_entry', [
                        [
                            1,
                        ],
                        [
                            2,
                        ],
                        [],
                    ]),
                ),
            ),
        );

        $this->assertEquals(
            [1, 2],
            $rows->first()->valueOf('element')
        );
    }
}
