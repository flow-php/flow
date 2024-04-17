<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;

final class ReaderTest extends TestCase
{
    public function test_reading_required_columns() : void
    {
        // File generated with https://gist.github.com/norberttech/01322f61dca77cfde5161e31e94463ef
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/columns.required.parquet');

        $rows = 0;

        foreach ($file->values() as $row) {
            foreach ($row as $column => $value) {
                self::assertNotNull($value);
            }
            $rows++;
        }

        self::assertSame(100, $rows);
    }
}
