<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StringWidthTest extends FlowTestCase
{
    public function test_width_ascii_string(): void
    {
        static::assertSame(5, ref('str')
            ->stringWidth()
            ->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context()));
    }

    public function test_width_empty_string(): void
    {
        static::assertSame(0, ref('str')
            ->stringWidth()
            ->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context()));
    }

    public function test_width_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringWidth function requires non-null value');

        ref('str')
            ->stringWidth()
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());
    }

    public function test_width_single_character(): void
    {
        static::assertSame(1, ref('str')
            ->stringWidth()
            ->eval(array_to_row(['str' => 'a'], schema(str_schema('str'))), flow_context()));
    }
}
