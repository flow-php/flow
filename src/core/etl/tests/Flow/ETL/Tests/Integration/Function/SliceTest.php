<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class SliceTest extends FlowTestCase
{
    public function test_slice() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world', 'start' => 1, 'length' => 3],
                        ['text' => 'hello world', 'start' => -5, 'length' => null],
                        ['text' => 'café au lait', 'start' => 1, 'length' => 3],
                        ['text' => 'hello🚀world', 'start' => 2, 'length' => 5],
                        ['text' => '', 'start' => 0, 'length' => 5],
                        ['text' => null, 'start' => 1, 'length' => 3],
                    ]
                )
            )
            ->withEntry('sliced', ref('text')->slice(ref('start'), ref('length')))
            ->withEntry('sliced_no_length', ref('text')->slice(ref('start')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'start' => 1, 'length' => 3, 'sliced' => 'ell', 'sliced_no_length' => 'ello world'],
                ['text' => 'hello world', 'start' => -5, 'length' => null, 'sliced' => 'world', 'sliced_no_length' => 'world'],
                ['text' => 'café au lait', 'start' => 1, 'length' => 3, 'sliced' => 'afé', 'sliced_no_length' => 'afé au lait'],
                ['text' => 'hello🚀world', 'start' => 2, 'length' => 5, 'sliced' => 'llo🚀w', 'sliced_no_length' => 'llo🚀world'],
                ['text' => '', 'start' => 0, 'length' => 5, 'sliced' => '', 'sliced_no_length' => ''],
                ['text' => null, 'start' => 1, 'length' => 3, 'sliced' => null, 'sliced_no_length' => null],
            ],
            $memory->dump()
        );
    }

    public function test_slice_edge_cases() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world'],
                        ['text' => 'a'],
                        ['text' => ''],
                    ]
                )
            )
            ->withEntry('slice_beyond_length', ref('text')->slice(20))
            ->withEntry('slice_negative_start', ref('text')->slice(-50))
            ->withEntry('slice_negative_length', ref('text')->slice(0, -3))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'slice_beyond_length' => '', 'slice_negative_start' => 'hello world', 'slice_negative_length' => 'hello wo'],
                ['text' => 'a', 'slice_beyond_length' => '', 'slice_negative_start' => 'a', 'slice_negative_length' => ''],
                ['text' => '', 'slice_beyond_length' => '', 'slice_negative_start' => '', 'slice_negative_length' => ''],
            ],
            $memory->dump()
        );
    }

    public function test_slice_with_static_values() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world test'],
                        ['text' => 'café au lait'],
                        ['text' => 'नमस्ते दुनिया'],
                        ['text' => 'hello🚀world'],
                    ]
                )
            )
            ->withEntry('slice_start', ref('text')->slice(1, 3))
            ->withEntry('slice_negative', ref('text')->slice(-5))
            ->withEntry('slice_zero_length', ref('text')->slice(1, 0))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world test', 'slice_start' => 'ell', 'slice_negative' => ' test', 'slice_zero_length' => ''],
                ['text' => 'café au lait', 'slice_start' => 'afé', 'slice_negative' => ' lait', 'slice_zero_length' => ''],
                ['text' => 'नमस्ते दुनिया', 'slice_start' => 'मस्ते ', 'slice_negative' => 'स्ते दुनिया', 'slice_zero_length' => ''],
                ['text' => 'hello🚀world', 'slice_start' => 'ell', 'slice_negative' => 'world', 'slice_zero_length' => ''],
            ],
            $memory->dump()
        );
    }
}
