<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\ArraySortTransformer;
use PHPUnit\Framework\TestCase;

final class ArraySortTransformerTest extends TestCase
{
    public function test_sort_integer_array_entries() : void
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

        $transformer = new ArraySortTransformer('array', \SORT_REGULAR);

        $this->assertSame(
            [
                [
                    'array' => [3, 4, 5, 10],
                ],
            ],
            $transformer->transform(new Rows(Row::create($arrayEntry)))->toArray()
        );
    }

    public function test_sort_datetime_array_entries() : void
    {
        $arrayEntry = new ArrayEntry(
            'array',
            [
                new \DateTimeImmutable('2020-01-10 00:00:00 UTC'),
                new \DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                new \DateTimeImmutable('2020-01-03 00:00:00 UTC'),
                new \DateTimeImmutable('2020-01-01 12:00:00 UTC'),
            ]
        );

        $transformer = new ArraySortTransformer('array', \SORT_REGULAR);

        $this->assertEquals(
            [
                [
                    'array' => [
                        new \DateTimeImmutable('2020-01-01 00:00:00 UTC'),
                        new \DateTimeImmutable('2020-01-01 12:00:00 UTC'),
                        new \DateTimeImmutable('2020-01-03 00:00:00 UTC'),
                        new \DateTimeImmutable('2020-01-10 00:00:00 UTC'),
                    ],
                ],
            ],
            $transformer->transform(new Rows(Row::create($arrayEntry)))->toArray()
        );
    }
}
