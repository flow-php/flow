<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\files;

final class PushesLimitTest extends FlowTestCase
{
    public function test_a_second_push_narrows_and_never_widens(): void
    {
        $widened = files(__DIR__ . '/Fixtures/FileListExtractor/*');
        $widened->pushLimit(10);
        $widened->pushLimit(100);

        $narrowed = files(__DIR__ . '/Fixtures/FileListExtractor/*');
        $narrowed->pushLimit(100);
        $narrowed->pushLimit(10);

        static::assertSame(10, $widened->pushedLimit());
        static::assertSame(10, $narrowed->pushedLimit());
    }

    public function test_nothing_is_pushed_by_default(): void
    {
        static::assertNull(files(__DIR__ . '/Fixtures/FileListExtractor/*')->pushedLimit());
    }

    #[TestWith([0])]
    #[TestWith([-1])]
    public function test_push_rejects_zero_and_negative(int $limit): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Limit must be greater than 0');

        files(__DIR__ . '/Fixtures/FileListExtractor/*')->pushLimit($limit);
    }
}
