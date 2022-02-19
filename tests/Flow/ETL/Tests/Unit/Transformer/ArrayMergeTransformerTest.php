<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\ArrayMergeTransformer;
use PHPUnit\Framework\TestCase;

final class ArrayMergeTransformerTest extends TestCase
{
    public function test_array_merge_entries() : void
    {
        $arrayOneEntry = new ArrayEntry(
            'array_one',
            [
                5,
                3,
                10,
                4,
            ]
        );
        $arrayTwoEntry = new ArrayEntry(
            'array_two',
            [
                'A',
                'Z',
                'C',
                'O',
            ]
        );

        $transformer = new ArrayMergeTransformer(['array_one', 'array_two']);

        $this->assertSame(
            [
                [
                    'array_one' => [5, 3, 10, 4],
                    'array_two' => ['A', 'Z', 'C', 'O'],
                    'merged' => [5, 3, 10, 4, 'A', 'Z', 'C', 'O'],
                ],
            ],
            $transformer->transform(new Rows(Row::create($arrayOneEntry, $arrayTwoEntry)))->toArray()
        );
    }
}
