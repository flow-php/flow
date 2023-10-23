<?php declare(strict_types=1);

namespace Flow\Dremel\Tests\Unit;

use function Flow\Parquet\array_flatten;
use Flow\Dremel\Dremel;
use Flow\Dremel\Exception\RuntimeException;
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

        $shredded = (new Dremel())->shred($data, 2);
        $this->assertSame(
            [
                'repetitions' => [0, 2, 2, 0, 2, 2, 1, 2, 2, 0, 2, 2],
                'definitions' => [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
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

    public function test_dremel_with_rows_with_combined_scalar_values_and_array_values() : void
    {
        $data = [
            [
                [0, 1, 2],
            ],
            [
                3, 4, 5,
                [3, 4, 5],
            ],
            [
                [6, 7, 8],
            ],
        ];

        $this->expectExceptionMessage('Invalid data structure, each row must be an array of arrays or scalars, got both, arrays and scalars. [3,4,5,[3,4,5]]');
        $this->expectException(RuntimeException::class);

        (new Dremel())->shred($data, 2);
    }
}
