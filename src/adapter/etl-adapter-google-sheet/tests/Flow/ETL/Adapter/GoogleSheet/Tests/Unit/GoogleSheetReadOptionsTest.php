<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetReadOptions;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Inference\InferredTypes;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Flow\Types\Type\Native\String\StringTypeNarrower;

use function array_map;

final class GoogleSheetReadOptionsTest extends FlowTestCase
{
    public function test_defaults(): void
    {
        $options = new GoogleSheetReadOptions();

        static::assertTrue($options->withHeader);
        static::assertTrue($options->dropExtraColumns);
        static::assertTrue($options->emptyToNull);
        static::assertSame([], $options->options);
    }

    public function test_with_header_changes_only_the_header_flag(): void
    {
        $options = (new GoogleSheetReadOptions())->withHeader(false);

        static::assertFalse($options->withHeader);
        static::assertTrue($options->dropExtraColumns);
        static::assertTrue($options->emptyToNull);
        static::assertSame([], $options->options);
    }

    public function test_with_drop_extra_columns_changes_only_that_flag(): void
    {
        $options = (new GoogleSheetReadOptions())->withDropExtraColumns(false);

        static::assertTrue($options->withHeader);
        static::assertFalse($options->dropExtraColumns);
        static::assertTrue($options->emptyToNull);
        static::assertSame([], $options->options);
    }

    public function test_with_empty_to_null_changes_only_that_flag(): void
    {
        $options = (new GoogleSheetReadOptions())->withEmptyToNull(false);

        static::assertTrue($options->withHeader);
        static::assertTrue($options->dropExtraColumns);
        static::assertFalse($options->emptyToNull);
        static::assertSame([], $options->options);
    }

    public function test_with_options_changes_only_the_options(): void
    {
        $options = (new GoogleSheetReadOptions())->withOptions(['valueRenderOption' => 'UNFORMATTED_VALUE']);

        static::assertTrue($options->withHeader);
        static::assertTrue($options->dropExtraColumns);
        static::assertTrue($options->emptyToNull);
        static::assertSame(['valueRenderOption' => 'UNFORMATTED_VALUE'], $options->options);
    }

    public function test_encoder_is_a_fresh_instance_per_call(): void
    {
        $options = new GoogleSheetReadOptions();

        static::assertNotSame($options->encoder(), $options->encoder());
    }

    public function test_encoder_carries_the_empty_to_null_flag(): void
    {
        static::assertSame(
            [['a' => '']],
            array_map(static fn($rowValues): array => $rowValues->values, (new GoogleSheetReadOptions())
                ->withEmptyToNull(false)
                ->encoder()
                ->decode([['a'], ['']])),
        );
    }

    public function test_encoder_carries_the_drop_extra_columns_flag(): void
    {
        $encoder = (new GoogleSheetReadOptions())
            ->withDropExtraColumns(false)
            ->encoder();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row has more columns (2) than headers (1)');

        $encoder->decode([['a'], ['x', 'y']]);
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

    public function test_encoder_carries_the_header_flag(): void
    {
        $encoder = (new GoogleSheetReadOptions())
            ->withHeader(false)
            ->encoder();
        $encoder->decode([['1']]);

        static::assertSame(['e00'], $encoder->headers());
    }
}
