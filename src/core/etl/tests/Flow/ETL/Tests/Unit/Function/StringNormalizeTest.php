<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Normalizer;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class StringNormalizeTest extends FlowTestCase
{
    public function test_a_malformed_form_operand_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ref('value')->stringNormalize(lit('not-a-form'))->eval(row(str_entry('value', 'abc')), flow_context());
    }

    public function test_normalize_already_normalized(): void
    {
        static::assertSame('hello', ref('str')
            ->stringNormalize(Normalizer::NFC)
            ->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_normalize_empty_string(): void
    {
        static::assertSame('', ref('str')->stringNormalize()->eval(row(str_entry('str', '')), flow_context()));
    }

    public function test_normalize_nfc_default(): void
    {
        static::assertSame('é', ref('str')
            ->stringNormalize()
            ->eval(row(str_entry('str', "e\u{0301}")), flow_context()));
    }

    public function test_normalize_nfc_explicit(): void
    {
        static::assertSame('é', ref('str')
            ->stringNormalize(Normalizer::NFC)
            ->eval(row(str_entry('str', "e\u{0301}")), flow_context()));
    }

    public function test_normalize_nfd(): void
    {
        static::assertSame("e\u{0301}", ref('str')
            ->stringNormalize(Normalizer::NFD)
            ->eval(row(str_entry('str', 'é')), flow_context()));
    }

    public function test_normalize_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringNormalize function requires non-null value');

        ref('str')->stringNormalize()->eval(row(str_entry('str', null)), flow_context());
    }

    public function test_normalize_with_scalar_function_form(): void
    {
        $normalized = ref('str')
            ->stringNormalize(ref('form'))
            ->eval(row(str_entry('str', "e\u{0301}"), int_entry('form', Normalizer::NFC)), flow_context());

        static::assertSame('é', $normalized);
    }
}
