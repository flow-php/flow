<?php declare(strict_types=1);

namespace Flow\Dremel\Tests\Unit;

use function Flow\Parquet\array_flatten;
use Flow\Dremel\Dremel;
use PHPUnit\Framework\TestCase;

final class DremelShredTest extends TestCase
{
    public function test_dremel_shred_on_a_flat_non_nullable_columns() : void
    {
        $data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        $shredded = (new Dremel())->shred($data, 10);
        $this->assertEquals(
            [
                'repetitions' => [],
                'definitions' => [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],
                'values' => $data,
            ],
            [
                'repetitions' => $shredded->repetitions,
                'definitions' => $shredded->definitions,
                'values' => $shredded->values,
            ]
        );
    }

    public function test_dremel_shred_on_deeply_not_equally_distributed_repeated_columns() : void
    {
        $data = [
            [
                [0, 1, 2],
            ],
            [
                [3, 4, 5],
                [3, 4, 5],
            ],
            [
                [6, 7, 8],
            ],
        ];

        $shredded = (new Dremel())->shred($data, 5);
        $this->assertSame(
            [
                'repetitions' => [0, 2, 2, 0, 2, 2, 1, 2, 2, 0, 2, 2],
                'definitions' => [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5],
                'values' => array_flatten($data),
            ],
            [
                'repetitions' => $shredded->repetitions,
                'definitions' => $shredded->definitions,
                'values' => $shredded->values,
            ]
        );
    }

    public function test_dremel_shred_on_deeply_repeated_columns() : void
    {
        $data = [
            [[0, 1, 2]],
            [[3, 4, 5]],
            [[6, 7, 8]],
        ];

        $shredded = (new Dremel())->shred($data, 2);
        $this->assertSame(
            [
                'repetitions' => [0, 2, 2, 0, 2, 2, 0, 2, 2],
                'definitions' => [2, 2, 2, 2, 2, 2, 2, 2, 2],
                'values' => array_flatten($data),
            ],
            [
                'repetitions' => $shredded->repetitions,
                'definitions' => $shredded->definitions,
                'values' => $shredded->values,
            ]
        );
    }

    public function test_dremel_shred_on_nested_nullable_lists() : void
    {
        $data = [[1, 2, 3], [null, null], [4, 5, 6]];

        $shredded = (new Dremel())->shred($data, 3);
        $this->assertSame(
            [
                'repetitions' => [0, 1, 1, 0, 1, 0, 1, 1],
                'definitions' => [3, 3, 3, 2, 2, 3, 3, 3],
                'values' => [1, 2, 3, 4, 5, 6],
            ],
            [
                'repetitions' => $shredded->repetitions,
                'definitions' => $shredded->definitions,
                'values' => $shredded->values,
            ]
        );
    }

    public function test_dremel_shred_on_repeated_columns() : void
    {
        $data = [
            [0, 1, 2],
            [3, 4, 5],
            [6, 7, 8],
        ];

        $shredded = (new Dremel())->shred($data, 2);
        $this->assertSame(
            [
                'repetitions' => [0, 1, 1, 0, 1, 1, 0, 1, 1],
                'definitions' => [2, 2, 2, 2, 2, 2, 2, 2, 2],
                'values' => array_flatten($data),
            ],
            [
                'repetitions' => $shredded->repetitions,
                'definitions' => $shredded->definitions,
                'values' => $shredded->values,
            ]
        );
    }
}
