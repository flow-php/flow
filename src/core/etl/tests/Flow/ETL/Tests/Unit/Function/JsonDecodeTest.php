<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use JsonException;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class JsonDecodeTest extends FlowTestCase
{
    public function test_json_decode_expression(): void
    {
        static::assertSame(
            ['value' => 1],
            (new FunctionContext(flow_context()))->eval(
                ref('value')->jsonDecode(),
                ['value' => '{"value": 1}'],
                schema(str_schema('value')),
            ),
        );
    }

    public function test_json_decode_expression_with_invalid_json(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('JsonDecode error: Syntax error');

        (new FunctionContext(flow_context()))->eval(
            ref('value')->jsonDecode(),
            ['value' => '{"value": 1'],
            schema(str_schema('value')),
        );
    }

    public function test_invalid_json_keeps_the_decoder_error_as_the_cause(): void
    {
        try {
            (new FunctionContext(flow_context()))->eval(
                ref('value')->jsonDecode(),
                ['value' => '{"value": 1'],
                schema(str_schema('value')),
            );
            static::fail('Invalid JSON must be refused.');
        } catch (EvaluationException $e) {
            static::assertInstanceOf(JsonException::class, $e->getPrevious()?->getPrevious());
        }
    }

    public function test_json_decode_on_non_json_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('JsonDecode function requires string, array, or Json value');

        (new FunctionContext(flow_context()))->eval(
            ref('value')->jsonDecode(),
            ['value' => 125],
            schema(int_schema('value')),
        );
    }

    public function test_json_decode_on_scalar_json_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'JsonDecode function requires JSON that decodes to an array, cast scalar JSON instead',
        );

        (new FunctionContext(flow_context()))->eval(
            ref('value')->jsonDecode(),
            ['value' => '5'],
            schema(str_schema('value')),
        );
    }
}
