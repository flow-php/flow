<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function file_exists;
use function Flow\ETL\DSL\generate_random_string;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function iterator_to_array;
use function mkdir;
use function unlink;

#[Group('native-extension')]
final class CrossEngineTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('arrow')) {
            self::markTestSkipped('Arrow extension is not loaded');
        }

        if (!file_exists(__DIR__ . '/var')) {
            mkdir(__DIR__ . '/var');
        }
    }

    public function test_arrow_write_arrow_read_roundtrip(): void
    {
        $path = __DIR__ . '/var/test-cross-arrow-arrow-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            FlatColumn::string('label'),
            FlatColumn::int64('big_number'),
        );

        $inputData = [
            ['id' => 1, 'label' => 'first', 'big_number' => 1_000_000_000_000],
            ['id' => 2, 'label' => 'second', 'big_number' => 2_000_000_000_000],
        ];

        $engine = new ArrowParquetEngine();

        $writeStream = NativeLocalDestinationStream::openBlank(path($path));
        $engine->writeRows($writeStream, $schema, Compressions::SNAPPY, new Options(), $inputData);

        $parquetFile = (new Reader())->read($path);
        $readStream = NativeLocalSourceStream::open(path_real($path));
        $result = iterator_to_array($engine->readValues($readStream, $parquetFile->schema()));

        static::assertCount(2, $result);
        static::assertSame(1, $result[0]['id']);
        static::assertSame('first', $result[0]['label']);
        static::assertSame(1_000_000_000_000, $result[0]['big_number']);

        unlink($path);
    }

    public function test_arrow_write_php_read(): void
    {
        $path = __DIR__ . '/var/test-cross-arrow-php-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            FlatColumn::string('name'),
            FlatColumn::boolean('active'),
        );

        $inputData = [
            ['id' => 10, 'name' => 'alpha', 'active' => true],
            ['id' => 20, 'name' => 'beta', 'active' => false],
        ];

        $engine = new ArrowParquetEngine();
        $stream = NativeLocalDestinationStream::openBlank(path($path));
        $engine->writeRows($stream, $schema, Compressions::SNAPPY, new Options(), $inputData);

        $result = iterator_to_array(
            (new Reader(engine: new PhpParquetEngine()))
                ->read($path)
                ->values(),
        );

        static::assertCount(2, $result);
        static::assertSame(10, $result[0]['id']);
        static::assertSame('alpha', $result[0]['name']);
        static::assertTrue($result[0]['active']);
        static::assertSame(20, $result[1]['id']);
        static::assertFalse($result[1]['active']);

        unlink($path);
    }

    public function test_explicit_php_engine(): void
    {
        $path = __DIR__ . '/var/test-php-engine-' . generate_random_string() . '.parquet';

        $schema = Schema::with(FlatColumn::int32('id', Repetition::REQUIRED), FlatColumn::string('name'));

        $inputData = [
            ['id' => 1, 'name' => 'forced-php'],
        ];

        $phpEngine = new PhpParquetEngine();

        (new Writer(engine: $phpEngine))->write($path, $schema, $inputData);

        $result = iterator_to_array(
            (new Reader(engine: $phpEngine))
                ->read($path)
                ->values(),
        );

        static::assertCount(1, $result);
        static::assertSame(1, $result[0]['id']);
        static::assertSame('forced-php', $result[0]['name']);

        unlink($path);
    }

    public function test_php_write_arrow_read(): void
    {
        $path = __DIR__ . '/var/test-cross-php-arrow-' . generate_random_string() . '.parquet';

        $schema = Schema::with(
            FlatColumn::int32('id', Repetition::REQUIRED),
            FlatColumn::string('name'),
            FlatColumn::double('value'),
        );

        $inputData = [
            ['id' => 1, 'name' => 'one', 'value' => 1.1],
            ['id' => 2, 'name' => 'two', 'value' => 2.2],
            ['id' => 3, 'name' => 'three', 'value' => 3.3],
        ];

        (new Writer(engine: new PhpParquetEngine()))->write($path, $schema, $inputData);

        $engine = new ArrowParquetEngine();
        $parquetFile = (new Reader())->read($path);
        $result = iterator_to_array($engine->readValues(
            NativeLocalSourceStream::open(path_real($path)),
            $parquetFile->schema(),
        ));

        static::assertCount(3, $result);
        static::assertSame(1, $result[0]['id']);
        static::assertSame('one', $result[0]['name']);
        static::assertEqualsWithDelta(1.1, $result[0]['value'], 0.001);

        unlink($path);
    }
}
