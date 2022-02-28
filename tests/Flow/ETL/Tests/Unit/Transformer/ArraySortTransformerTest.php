<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DSL\Transform;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ArraySortTransformerTest extends TestCase
{
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

        $transformer = Transform::array_sort('array');

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

        $transformer = Transform::array_sort('array');

        $this->assertSame(
            [
                [
                    'array' => [3, 4, 5, 10],
                ],
            ],
            $transformer->transform(new Rows(Row::create($arrayEntry)))->toArray()
        );
    }
}
