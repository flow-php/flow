<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\File;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\File\FileScan;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

final class FileScanTest extends FlowTestCase
{
    public function test_a_default_scan_pushes_nothing(): void
    {
        $scan = new FileScan();

        static::assertNull($scan->limit);
        static::assertInstanceOf(OnlyFiles::class, $scan->pathFilter);
        static::assertNull($scan->filter);
    }

    #[TestWith([0])]
    #[TestWith([-1])]
    public function test_a_limit_below_one_is_refused(int $limit): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Limit must be greater than 0');

        new FileScan($limit);
    }

    public function test_a_limit_beside_a_filter_is_kept(): void
    {
        $filter = ref('id')->greaterThan(lit(1));
        $scan = new FileScan(5, filter: $filter);

        static::assertSame(5, $scan->limit);
        static::assertSame($filter, $scan->filter);
    }

    public function test_a_filter_without_a_limit_is_kept(): void
    {
        $filter = ref('id')->greaterThan(lit(1));

        static::assertSame($filter, (new FileScan(filter: $filter))->filter);
    }
}
