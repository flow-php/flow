<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Normalizer;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StringNormalizeTest extends FlowTestCase
{
    public function test_a_malformed_form_operand_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new FunctionContext(flow_context()))->eval(
            ref('value')->stringNormalize(lit('not-a-form')),
            ['value' => 'abc'],
            schema(str_schema('value')),
        );
    }

    public function test_normalize_already_normalized(): void
    {
        static::assertSame('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringNormalize(Normalizer::NFC),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_normalize_empty_string(): void
    {
        static::assertSame('', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringNormalize(),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_normalize_nfc_default(): void
    {
        static::assertSame('é', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringNormalize(),
            ['str' => "e\u{0301}"],
            schema(str_schema('str')),
        ));
    }

    public function test_normalize_nfc_explicit(): void
    {
        static::assertSame('é', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringNormalize(Normalizer::NFC),
            ['str' => "e\u{0301}"],
            schema(str_schema('str')),
        ));
    }

    public function test_normalize_nfd(): void
    {
        static::assertSame("e\u{0301}", (new FunctionContext(flow_context()))->eval(
            ref('str')->stringNormalize(Normalizer::NFD),
            ['str' => 'é'],
            schema(str_schema('str')),
        ));
    }

    public function test_normalize_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringNormalize function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->stringNormalize(),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }

    public function test_normalize_with_scalar_function_form(): void
    {
        static::assertSame('é', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringNormalize(ref('form')),
            ['str' => "e\u{0301}", 'form' => Normalizer::NFC],
            schema(str_schema('str'), int_schema('form')),
        ));
    }
}
