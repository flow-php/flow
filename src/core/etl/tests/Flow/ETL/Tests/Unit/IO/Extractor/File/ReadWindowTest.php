<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ReadWindowTest extends FlowTestCase
{
    public function test_a_negative_offset_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ReadWindow offset must be 0 or more, got -1');

        new ReadWindow(offset: -1);
    }

    public function test_a_negative_limit_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ReadWindow limit must be 0 or more, got -1');

        new ReadWindow(limit: -1);
    }

    public function test_the_default_window_skips_nothing_and_wants_everything(): void
    {
        $window = new ReadWindow();

        static::assertSame(0, $window->offset);
        static::assertNull($window->limit);
    }

    public function test_trim_cuts_rows_to_what_the_limit_still_wants(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        static::assertSame(
            [['id' => 1]],
            (new ReadWindow(limit: 3))
                ->trim($rows, 2)
                ->toArray(),
        );
        static::assertSame($rows, (new ReadWindow(limit: 5))->trim($rows, 2));
        static::assertSame($rows, (new ReadWindow())->trim($rows, 100));
    }

    public function test_skip_drops_what_the_offset_still_skips(): void
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));

        static::assertSame(
            [['id' => 3]],
            (new ReadWindow(offset: 4))
                ->skip($rows, 2)
                ->toArray(),
        );
        static::assertSame(
            [],
            (new ReadWindow(offset: 4))
                ->skip($rows, 0)
                ->toArray(),
        );
        static::assertSame(
            [],
            (new ReadWindow(offset: 3))
                ->skip($rows, 0)
                ->toArray(),
        );
        static::assertSame($rows, (new ReadWindow(offset: 2))->skip($rows, 2));
        static::assertSame($rows, (new ReadWindow())->skip($rows, 0));
    }

    public function test_reached_only_once_the_limit_was_yielded(): void
    {
        static::assertFalse((new ReadWindow(limit: 3))->reached(2));
        static::assertTrue((new ReadWindow(limit: 3))->reached(3));
        static::assertFalse((new ReadWindow())->reached(1_000));
    }
}
