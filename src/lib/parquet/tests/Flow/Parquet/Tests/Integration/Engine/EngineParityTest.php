<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function file_exists;
use function Flow\ETL\DSL\generate_random_string;
use function iterator_to_array;
use function mkdir;
use function unlink;

#[Group('native-extension')]
final class EngineParityTest extends TestCase
{
    private string $path = '';

    protected function setUp(): void
    {
        if (!extension_loaded('arrow')) {
            self::markTestSkipped('Arrow extension is not loaded');
        }

        if (!file_exists(__DIR__ . '/var')) {
            mkdir(__DIR__ . '/var');
        }

        $this->path = __DIR__ . '/var/engine-parity-' . generate_random_string() . '.parquet';
    }

    protected function tearDown(): void
    {
        if ($this->path !== '' && file_exists($this->path)) {
            unlink($this->path);
        }
    }

    /**
     * Every other f32 test has each engine read back only what it wrote, so the two never had to
     * agree.
     */
    public function test_both_engines_decode_the_same_float32_bytes_identically(): void
    {
        (new Writer())->write($this->path, Schema::with(FlatColumn::float('v')), [
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
            iterator_to_array(Reader::arrow()->read($this->path)->values()),
            iterator_to_array(Reader::php()->read($this->path)->values()),
        );
    }
}
