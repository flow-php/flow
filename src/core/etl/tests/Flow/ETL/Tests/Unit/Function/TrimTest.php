<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Trim\Type;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class TrimTest extends FlowTestCase
{
    public function test_trim_both_valid_string(): void
    {
        static::assertSame('value', ref('string')
            ->trim()
            ->eval(array_to_row(['string' => '   value'], schema(str_schema('string'))), flow_context()));
    }

    public function test_trim_left_valid_string(): void
    {
        static::assertSame('value   ', ref('string')
            ->trim(Type::LEFT)
            ->eval(array_to_row(['string' => '   value   '], schema(str_schema('string'))), flow_context()));
    }

    public function test_trim_right_valid_string(): void
    {
        static::assertSame('   value', ref('string')
            ->trim(Type::RIGHT)
            ->eval(array_to_row(['string' => '   value   '], schema(str_schema('string'))), flow_context()));
    }
}
