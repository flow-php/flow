<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\GoogleSheet\IO\Extractor\GoogleSheetReadOptions;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Flow\Types\Type\Native\String\StringTypeNarrower;

final class GoogleSheetReadOptionsTest extends FlowTestCase
{
    public function test_defaults(): void
    {
        $options = new GoogleSheetReadOptions();

        static::assertTrue($options->withHeader);
        static::assertFalse($options->missingColumnsAsNull);
        static::assertTrue($options->emptyToNull);
        static::assertSame([], $options->options);
    }

    public function test_with_header_changes_only_the_header_flag(): void
    {
        $options = (new GoogleSheetReadOptions())->withHeader(false);

        static::assertFalse($options->withHeader);
        static::assertFalse($options->missingColumnsAsNull);
        static::assertTrue($options->emptyToNull);
        static::assertSame([], $options->options);
    }

    public function test_with_missing_columns_as_null_changes_only_that_flag(): void
    {
        $options = (new GoogleSheetReadOptions())->withMissingColumnsAsNull(true);

        static::assertTrue($options->withHeader);
        static::assertTrue($options->missingColumnsAsNull);
        static::assertTrue($options->emptyToNull);
        static::assertSame([], $options->options);
    }

    public function test_with_empty_to_null_changes_only_that_flag(): void
    {
        $options = (new GoogleSheetReadOptions())->withEmptyToNull(false);

        static::assertTrue($options->withHeader);
        static::assertFalse($options->missingColumnsAsNull);
        static::assertFalse($options->emptyToNull);
        static::assertSame([], $options->options);
    }

    public function test_with_options_changes_only_the_options(): void
    {
        $options = (new GoogleSheetReadOptions())->withOptions(['valueRenderOption' => 'UNFORMATTED_VALUE']);

        static::assertTrue($options->withHeader);
        static::assertFalse($options->missingColumnsAsNull);
        static::assertTrue($options->emptyToNull);
        static::assertSame(['valueRenderOption' => 'UNFORMATTED_VALUE'], $options->options);
    }

    public function test_decoder_is_a_fresh_instance_per_call(): void
    {
        $options = new GoogleSheetReadOptions();

        static::assertNotSame($options->decoder(), $options->decoder());
    }

    public function test_decoder_carries_the_empty_to_null_flag(): void
    {
        $decoder = (new GoogleSheetReadOptions())
            ->withEmptyToNull(false)
            ->decoder();
        $decoder->decode(['a']);

        static::assertSame(['a' => ''], $decoder->decode(['']));
    }

    public function test_decoder_carries_the_missing_columns_as_null_flag(): void
    {
        $decoder = (new GoogleSheetReadOptions())
            ->withMissingColumnsAsNull(true)
            ->decoder();
        $decoder->decode(['a', 'b']);

        static::assertSame(['a' => 'x', 'b' => null], $decoder->decode(['x']));
    }

    public function test_typer_runs_the_string_ladder_under_the_default_render_option(): void
    {
        static::assertInstanceOf(
            StringTypeNarrower::class,
            (new GoogleSheetReadOptions())->typer(InferredTypes::default()),
        );
    }

    public function test_typer_uses_the_value_type_under_any_other_render_option(): void
    {
        static::assertInstanceOf(
            InstanceOfTypeNarrower::class,
            (new GoogleSheetReadOptions(options: ['valueRenderOption' => 'UNFORMATTED_VALUE']))->typer(
                InferredTypes::default(),
            ),
        );
    }

    public function test_decoder_carries_the_header_flag(): void
    {
        $decoder = (new GoogleSheetReadOptions())
            ->withHeader(false)
            ->decoder();
        $decoder->decode([['1']]);

        static::assertSame(['e00'], $decoder->headers());
    }
}
