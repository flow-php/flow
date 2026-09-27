<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Normalizer;

use function Flow\ETL\DSL\array_to_row;
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

        ref('value')
            ->stringNormalize(lit('not-a-form'))
            ->eval(array_to_row(['value' => 'abc'], schema(str_schema('value'))), flow_context());
    }

    public function test_normalize_already_normalized(): void
    {
        static::assertSame('hello', ref('str')
            ->stringNormalize(Normalizer::NFC)
            ->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context()));
    }

    public function test_normalize_empty_string(): void
    {
        static::assertSame('', ref('str')
            ->stringNormalize()
            ->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context()));
    }

    public function test_normalize_nfc_default(): void
    {
        static::assertSame('é', ref('str')
            ->stringNormalize()
            ->eval(array_to_row(['str' => "e\u{0301}"], schema(str_schema('str'))), flow_context()));
    }

    public function test_normalize_nfc_explicit(): void
    {
        static::assertSame('é', ref('str')
            ->stringNormalize(Normalizer::NFC)
            ->eval(array_to_row(['str' => "e\u{0301}"], schema(str_schema('str'))), flow_context()));
    }

    public function test_normalize_nfd(): void
    {
        static::assertSame("e\u{0301}", ref('str')
            ->stringNormalize(Normalizer::NFD)
            ->eval(array_to_row(['str' => 'é'], schema(str_schema('str'))), flow_context()));
    }

    public function test_normalize_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringNormalize function requires non-null value');

        ref('str')
            ->stringNormalize()
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());
    }

    public function test_normalize_with_scalar_function_form(): void
    {
        $normalized = ref('str')
            ->stringNormalize(ref('form'))
            ->eval(
                array_to_row(
                    ['str' => "e\u{0301}", 'form' => Normalizer::NFC],
                    schema(str_schema('str'), int_schema('form')),
                ),
                flow_context(),
            );

        static::assertSame('é', $normalized);
    }
}
