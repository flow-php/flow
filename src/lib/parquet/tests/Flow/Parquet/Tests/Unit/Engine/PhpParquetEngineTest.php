<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\PhpParquetFileWriter;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\Tests\Mother\ParquetFileWriterMother;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class PhpParquetEngineTest extends TestCase
{
    public function test_open_for_write_returns_a_new_writer_for_every_call(): void
    {
        $engine = new PhpParquetEngine();
        $memory = memory_filesystem();

        $first = ParquetFileWriterMother::open($engine, $memory->writeTo(path('memory://first.parquet')));
        $second = ParquetFileWriterMother::open($engine, $memory->writeTo(path('memory://second.parquet')));

        static::assertInstanceOf(PhpParquetFileWriter::class, $first);
        static::assertInstanceOf(PhpParquetFileWriter::class, $second);
        static::assertNotSame($first, $second);
    }

    public function test_configuration_round_trips_the_byte_order_and_the_options(): void
    {
        $engine = new PhpParquetEngine(ByteOrder::BIG_ENDIAN, Options::default()->set(Option::PAGE_SIZE_BYTES, 7));

        static::assertSame('N', $engine->normalize()['byte_order']);
        static::assertSame(7, $engine->normalize()['options']['PAGE_SIZE_BYTES']);
        static::assertEquals($engine, PhpParquetEngine::fromArray($engine->normalize()));
    }

    public function test_a_configuration_with_an_unknown_byte_order_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown byte order: MIDDLE');

        PhpParquetEngine::fromArray(['byte_order' => 'MIDDLE', 'options' => []]);
    }

    public function test_a_configuration_without_options_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Parquet engine configuration must hold an "options" array');

        PhpParquetEngine::fromArray(['byte_order' => 'V']);
    }
}
