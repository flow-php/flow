<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\File;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Tests\FlowTestCase;

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
}
