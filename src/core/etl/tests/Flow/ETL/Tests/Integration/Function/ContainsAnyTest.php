<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{from_array, ref, to_memory};
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ContainsAnyTest extends FlowTestCase
{
    public function test_contains_any() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world', 'needles' => ['hello', 'foo']],
                        ['text' => 'hello world', 'needles' => ['foo', 'bar']],
                        ['text' => 'hello world', 'needles' => ['world', 'test']],
                        ['text' => 'hello world', 'needles' => []],
                        ['text' => '', 'needles' => ['hello']],
                        ['text' => null, 'needles' => ['hello']],
                        ['text' => 'hello world', 'needles' => null],
                        ['text' => 'नमस्ते', 'needles' => ['स्ते', 'foo']],
                        ['text' => 'hello🚀world', 'needles' => ['🚀', 'bar']],
                        ['text' => 'testing', 'needles' => ['test', 'ing']],
                    ]
                )
            )
            ->withEntry('contains_any', ref('text')->containsAny(ref('needles')))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'needles' => ['hello', 'foo'], 'contains_any' => true],
                ['text' => 'hello world', 'needles' => ['foo', 'bar'], 'contains_any' => false],
                ['text' => 'hello world', 'needles' => ['world', 'test'], 'contains_any' => true],
                ['text' => 'hello world', 'needles' => [], 'contains_any' => false],
                ['text' => '', 'needles' => ['hello'], 'contains_any' => false],
                ['text' => null, 'needles' => ['hello'], 'contains_any' => false],
                ['text' => 'hello world', 'needles' => null, 'contains_any' => false],
                ['text' => 'नमस्ते', 'needles' => ['स्ते', 'foo'], 'contains_any' => true],
                ['text' => 'hello🚀world', 'needles' => ['🚀', 'bar'], 'contains_any' => true],
                ['text' => 'testing', 'needles' => ['test', 'ing'], 'contains_any' => true],
            ],
            $memory->dump()
        );
    }

    public function test_contains_any_filtering() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['message' => 'error: file not found', 'level' => 'error'],
                        ['message' => 'warning: deprecated function', 'level' => 'warning'],
                        ['message' => 'info: process started', 'level' => 'info'],
                        ['message' => 'debug: variable value', 'level' => 'debug'],
                        ['message' => 'critical system failure', 'level' => 'critical'],
                        ['message' => 'notice: cache cleared', 'level' => 'notice'],
                    ]
                )
            )
            ->filter(ref('message')->containsAny(['error', 'warning', 'critical']))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['message' => 'error: file not found', 'level' => 'error'],
                ['message' => 'warning: deprecated function', 'level' => 'warning'],
                ['message' => 'critical system failure', 'level' => 'critical'],
            ],
            $memory->dump()
        );
    }

    public function test_contains_any_tag_filtering_use_case() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['content' => 'Check out this amazing product! #sale #discount', 'user' => 'seller1'],
                        ['content' => 'Just had lunch with friends #food #social', 'user' => 'user1'],
                        ['content' => 'New blog post about programming #tech #development', 'user' => 'dev1'],
                        ['content' => 'Beautiful sunset today #nature #photography', 'user' => 'photo1'],
                        ['content' => 'Flash sale ending soon! #urgent #sale #limited', 'user' => 'seller2'],
                        ['content' => 'Learning new programming concepts #education #tech', 'user' => 'student1'],
                    ]
                )
            )
            ->withEntry('is_promotional', ref('content')->containsAny(['#sale', '#discount', '#urgent']))
            ->withEntry('is_tech_related', ref('content')->containsAny(['#tech', '#development', '#programming']))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['content' => 'Check out this amazing product! #sale #discount', 'user' => 'seller1', 'is_promotional' => true, 'is_tech_related' => false],
                ['content' => 'Just had lunch with friends #food #social', 'user' => 'user1', 'is_promotional' => false, 'is_tech_related' => false],
                ['content' => 'New blog post about programming #tech #development', 'user' => 'dev1', 'is_promotional' => false, 'is_tech_related' => true],
                ['content' => 'Beautiful sunset today #nature #photography', 'user' => 'photo1', 'is_promotional' => false, 'is_tech_related' => false],
                ['content' => 'Flash sale ending soon! #urgent #sale #limited', 'user' => 'seller2', 'is_promotional' => true, 'is_tech_related' => false],
                ['content' => 'Learning new programming concepts #education #tech', 'user' => 'student1', 'is_promotional' => false, 'is_tech_related' => true],
            ],
            $memory->dump()
        );
    }

    public function test_contains_any_vs_contains_comparison() : void
    {
        (data_frame())
            ->read(
                from_array(
                    [
                        ['text' => 'hello world'],
                        ['text' => 'goodbye world'],
                        ['text' => 'test message'],
                        ['text' => 'hello test'],
                        ['text' => 'foo bar'],
                    ]
                )
            )
            ->withEntry('contains_hello', ref('text')->contains('hello'))
            ->withEntry('contains_any_keywords', ref('text')->containsAny(['hello', 'test', 'foo']))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        self::assertSame(
            [
                ['text' => 'hello world', 'contains_hello' => true, 'contains_any_keywords' => true],
                ['text' => 'goodbye world', 'contains_hello' => false, 'contains_any_keywords' => false],
                ['text' => 'test message', 'contains_hello' => false, 'contains_any_keywords' => true],
                ['text' => 'hello test', 'contains_hello' => true, 'contains_any_keywords' => true],
                ['text' => 'foo bar', 'contains_hello' => false, 'contains_any_keywords' => true],
            ],
            $memory->dump()
        );
    }
}
