<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Normalizer;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringNormalizeTest extends FlowTestCase
{
    public function test_a_malformed_form_operand_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ref('value')->stringNormalize(lit('not-a-form'))->eval(row(['value' => 'abc']), flow_context());
    }

    public function test_normalize_already_normalized(): void
    {
        static::assertSame('hello', ref('str')
            ->stringNormalize(Normalizer::NFC)
            ->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_normalize_empty_string(): void
    {
        static::assertSame('', ref('str')->stringNormalize()->eval(row(['str' => '']), flow_context()));
    }

    public function test_normalize_nfc_default(): void
    {
        static::assertSame('é', ref('str')->stringNormalize()->eval(row(['str' => "e\u{0301}"]), flow_context()));
    }

    public function test_normalize_nfc_explicit(): void
    {
        static::assertSame('é', ref('str')
            ->stringNormalize(Normalizer::NFC)
            ->eval(row(['str' => "e\u{0301}"]), flow_context()));
    }

    public function test_normalize_nfd(): void
    {
        static::assertSame("e\u{0301}", ref('str')
            ->stringNormalize(Normalizer::NFD)
            ->eval(row(['str' => 'é']), flow_context()));
    }

    public function test_normalize_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringNormalize function requires non-null value');

        ref('str')->stringNormalize()->eval(row(['str' => null]), flow_context());
    }

    public function test_normalize_with_scalar_function_form(): void
    {
        $normalized = ref('str')
            ->stringNormalize(ref('form'))
            ->eval(row(['str' => "e\u{0301}", 'form' => Normalizer::NFC]), flow_context());

        static::assertSame('é', $normalized);
    }
}
