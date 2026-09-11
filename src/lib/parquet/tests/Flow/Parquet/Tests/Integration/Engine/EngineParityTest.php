<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function iterator_to_array;

#[Group('native-extension')]
final class EngineParityTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('arrow')) {
            self::markTestSkipped('Arrow extension is not loaded');
        }
    }

    protected function tearDown(): void
    {
        TestParquetFile::remove($this);
    }

    /**
     * Every other f32 test has each engine read back only what it wrote, so the two never had to
     * agree.
     */
    public function test_both_engines_decode_the_same_float32_bytes_identically(): void
    {
        $path = TestParquetFile::path($this);

        (new Writer())->write($path, Schema::with(FlatColumn::float('v')), [
            ['v' => 0.1],
            ['v' => 18.52],
            ['v' => 1 / 3],
            ['v' => -0.1],
            ['v' => 3.14159265358979],
            ['v' => 1.0e-8],
            ['v' => 1234567.75],
            ['v' => -2.5e10],
        ]);

        static::assertSame(
            iterator_to_array(Reader::arrow()->read($path)->values()),
            iterator_to_array(Reader::php()->read($path)->values()),
        );
    }
}
