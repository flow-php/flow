<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class EqualsToTest extends FlowTestCase
{
    public function test_equals_to() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello', 'compare' => 'hello'],
                        ['text' => 'hello', 'compare' => 'world'],
                        ['text' => 'hello', 'compare' => 'Hello'],
                        ['text' => '', 'compare' => ''],
                        ['text' => 'hello', 'compare' => ''],
                        ['text' => 'नमस्ते', 'compare' => 'नमस्ते'],
                        ['text' => 'नमस्ते', 'compare' => 'नमस्कार'],
                        ['text' => null, 'compare' => 'hello'],
                        ['text' => 'hello', 'compare' => null],
                        ['text' => '🚀🌟', 'compare' => '🚀🌟'],
                    ]
                )
            )
            ->withEntry('equals', ref('text')->equalsTo(ref('compare')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello', 'compare' => 'hello', 'equals' => true],
                ['text' => 'hello', 'compare' => 'world', 'equals' => false],
                ['text' => 'hello', 'compare' => 'Hello', 'equals' => false],
                ['text' => '', 'compare' => '', 'equals' => true],
                ['text' => 'hello', 'compare' => '', 'equals' => false],
                ['text' => 'नमस्ते', 'compare' => 'नमस्ते', 'equals' => true],
                ['text' => 'नमस्ते', 'compare' => 'नमस्कार', 'equals' => false],
                ['text' => null, 'compare' => 'hello', 'equals' => null],
                ['text' => 'hello', 'compare' => null, 'equals' => null],
                ['text' => '🚀🌟', 'compare' => '🚀🌟', 'equals' => true],
            ],
            $memory->dump()
        );
    }

    public function test_equals_to_filtering() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['status' => 'active', 'name' => 'John'],
                        ['status' => 'inactive', 'name' => 'Jane'],
                        ['status' => 'active', 'name' => 'Bob'],
                        ['status' => 'pending', 'name' => 'Alice'],
                        ['status' => 'active', 'name' => 'Charlie'],
                    ]
                )
            )
            ->filter(ref('status')->equalsTo('active'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['status' => 'active', 'name' => 'John'],
                ['status' => 'active', 'name' => 'Bob'],
                ['status' => 'active', 'name' => 'Charlie'],
            ],
            $memory->dump()
        );
    }

    public function test_equals_to_vs_contains_difference() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world'],
                        ['text' => 'hello'],
                        ['text' => 'world'],
                        ['text' => 'hello world hello'],
                    ]
                )
            )
            ->withEntry('equals_hello', ref('text')->equalsTo('hello'))
            ->withEntry('contains_hello', ref('text')->contains('hello'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'equals_hello' => false, 'contains_hello' => true],
                ['text' => 'hello', 'equals_hello' => true, 'contains_hello' => true],
                ['text' => 'world', 'equals_hello' => false, 'contains_hello' => false],
                ['text' => 'hello world hello', 'equals_hello' => false, 'contains_hello' => true],
            ],
            $memory->dump()
        );
    }

    public function test_equals_to_with_validation_use_case() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['user_input' => 'yes', 'expected' => 'yes'],
                        ['user_input' => 'YES', 'expected' => 'yes'],
                        ['user_input' => 'y', 'expected' => 'yes'],
                        ['user_input' => 'no', 'expected' => 'no'],
                        ['user_input' => 'NO', 'expected' => 'no'],
                        ['user_input' => 'n', 'expected' => 'no'],
                    ]
                )
            )
            ->withEntry('is_exact_match', ref('user_input')->equalsTo(ref('expected')))
            ->withEntry('is_valid', ref('user_input')->equalsTo('yes')->or(ref('user_input')->equalsTo('no')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['user_input' => 'yes', 'expected' => 'yes', 'is_exact_match' => true, 'is_valid' => true],
                ['user_input' => 'YES', 'expected' => 'yes', 'is_exact_match' => false, 'is_valid' => false],
                ['user_input' => 'y', 'expected' => 'yes', 'is_exact_match' => false, 'is_valid' => false],
                ['user_input' => 'no', 'expected' => 'no', 'is_exact_match' => true, 'is_valid' => true],
                ['user_input' => 'NO', 'expected' => 'no', 'is_exact_match' => false, 'is_valid' => false],
                ['user_input' => 'n', 'expected' => 'no', 'is_exact_match' => false, 'is_valid' => false],
            ],
            $memory->dump()
        );
    }
}
