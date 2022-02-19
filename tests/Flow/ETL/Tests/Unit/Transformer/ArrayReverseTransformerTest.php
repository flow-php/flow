<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\ArrayReverseTransformer;
use PHPUnit\Framework\TestCase;

final class ArrayReverseTransformerTest extends TestCase
{
    public function test_array_reverse() : void
    {
        $arrayEntry = new ArrayEntry(
            'array',
            [
                5,
                3,
                10,
                4,
            ]
        );

        $transformer = new ArrayReverseTransformer('array', \SORT_REGULAR);

        $this->assertSame(
            [
                [
                    'array' => [4, 10, 3, 5],
                ],
            ],
            $transformer->transform(new Rows(Row::create($arrayEntry)))->toArray()
        );
    }
}
