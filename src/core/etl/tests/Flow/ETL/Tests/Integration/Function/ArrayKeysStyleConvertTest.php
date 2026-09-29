<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_keys_style_convert;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_memory;

final class ArrayKeysStyleConvertTest extends FlowTestCase
{
    public function test_array_keys_style_convert(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1, 'array' => ['camelCased' => 1, 'snake_cased' => 2, 'space word' => 3]],
            ]))
            ->withEntry('array', array_keys_style_convert(ref('array'), 'camel'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'array' => ['camelCased' => 1, 'snakeCased' => 2, 'spaceWord' => 3]],
            ],
            $memory->dump(),
        );
    }

    public function test_array_keys_style_convert_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        $context = flow_context(config());
        (new FunctionContext($context))->eval(
            array_keys_style_convert(ref('string'), 'camel'),
            [
                'string' => 'test',
            ],
            schema(str_schema('string')),
        );
    }

    public function test_a_nested_convert_stores_the_converted_shape(): void
    {
        data_frame()
            ->read(from_array([
                ['id' => 1, 'array' => ['itemId' => 1, 'itemVariants' => [['variantName' => 'a']]]],
            ]))
            ->withEntry('array', array_keys_style_convert(ref('array'), 'snake'))
            ->write(to_memory($memory = new ArrayMemory()))
            ->run();

        static::assertSame(
            [
                ['id' => 1, 'array' => ['item_id' => 1, 'item_variants' => [['variant_name' => 'a']]]],
            ],
            $memory->dump(),
        );
    }
}
