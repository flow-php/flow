<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{data_frame, from_array, ref, to_memory};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

final class ExecutionModeTest extends FlowTestCase
{
    public function test_lenient_mode_allows_multiple_errors() : void
    {
        $memory = new ArrayMemory();

        (data_frame())
            ->read(
                from_array([
                    ['id' => 1, 'text' => 'hello', 'value' => '10'],
                    ['id' => 2, 'text' => null, 'value' => null],
                    ['id' => 3, 'text' => 'world', 'value' => '30'],
                ])
            )
            ->withEntry('upper', ref('text')->upper())
            ->withEntry('starts_h', ref('text')->startsWith('h'))
            ->withEntry('value_int', ref('value')->cast('int'))
            ->withEntry('contains_o', ref('text')->stringContainsAny(['o']))
            ->write(to_memory($memory))
            ->run();

        $result = $memory->dump();

        self::assertCount(3, $result);
        self::assertNull($result[1]['upper']);
        self::assertFalse($result[1]['starts_h']);
        self::assertNull($result[1]['value_int']);
        self::assertFalse($result[1]['contains_o']);
    }

    public function test_lenient_mode_with_invalid_data_continues_processing() : void
    {
        $memory = new ArrayMemory();

        (data_frame())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'John', 'value' => '10'],
                    ['id' => 2, 'name' => null, 'value' => '20'],
                    ['id' => 3, 'name' => 'Jane', 'value' => null],
                    ['id' => 4, 'name' => 'Bob', 'value' => '40'],
                ])
            )
            ->withEntry('name_upper', ref('name')->upper())
            ->withEntry('value_int', ref('value')->cast('int'))
            ->withEntry('starts_with_j', ref('name')->startsWith('J'))
            ->write(to_memory($memory))
            ->run();

        $result = $memory->dump();

        self::assertCount(4, $result);
        self::assertNull($result[1]['name_upper']);
        self::assertFalse($result[1]['starts_with_j']);
        self::assertNull($result[2]['value_int']);
    }

    public function test_strict_mode_throws_exception_on_invalid_data() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/StartsWith function requires non-null/');

        (data_frame())
            ->read(
                from_array([
                    ['id' => 1, 'name' => 'John'],
                    ['id' => 2, 'name' => null],
                    ['id' => 3, 'name' => 'Jane'],
                ])
            )
            ->mode(ExecutionMode::STRICT)
            ->withEntry('starts_with_j', ref('name')->startsWith('J'))
            ->fetch();
    }

    public function test_strict_mode_with_cast() : void
    {
        $this->expectException(InvalidArgumentException::class);

        (data_frame())
            ->read(
                from_array([
                    ['value' => '10'],
                    ['value' => null],
                ])
            )
            ->mode(ExecutionMode::STRICT)
            ->withEntry('value_int', ref('value')->cast('int'))
            ->fetch();
    }

    public function test_strict_mode_with_index_of() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IndexOf function requires non-null string and needle');

        (data_frame())
            ->read(
                from_array([
                    ['text' => 'hello world'],
                    ['text' => null],
                ])
            )
            ->mode(ExecutionMode::STRICT)
            ->withEntry('pos', ref('text')->indexOf('world'))
            ->fetch();
    }

    public function test_strict_mode_with_json_decode() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('JsonDecode function requires non-null value');

        (data_frame())
            ->read(
                from_array([
                    ['json' => '{"name":"John"}'],
                    ['json' => null],
                ])
            )
            ->mode(ExecutionMode::STRICT)
            ->withEntry('decoded', ref('json')->jsonDecode())
            ->fetch();
    }
}
