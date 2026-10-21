<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ValueStorage;

use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\Writer\ValueStorage\BufferValueStorage;
use PHPUnit\Framework\TestCase;

final class BufferValueStorageTest extends TestCase
{
    public function test_encoding(): void
    {
        static::assertSame(Encodings::PLAIN, (new BufferValueStorage())->encoding());
    }
}
