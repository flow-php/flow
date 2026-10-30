<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Mother\ReadPlanMother;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;

#[RequiresPhpExtension('arrow')]
final class RustParquetEngineTest extends TestCase
{
    public function test_int96_as_datetime_false_refuses_an_int96_column(): void
    {
        $engine = new RustParquetEngine(Options::default()->set(Option::INT_96_AS_DATETIME, false));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Parquet column "ts" holds INT96');

        $engine->openForRead(NativeLocalSourceStream::open(path_real(__DIR__
        . '/../../Integration/IO/Fixtures/EdgeCases/int96.parquet')))->readColumns(
            ['ts'],
            10,
            ReadPlanMother::window(2),
        );
    }

    public function test_lzo_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('LZO compression is not supported by the Arrow engine');

        (new RustParquetEngine())->openForWrite(
            new StringDestinationStream(path('memory://out.parquet')),
            Schema::with(FlatColumn::int32('id')),
            Compressions::LZO,
            new Options(),
        );
    }

    public function test_configuration_round_trips_the_options(): void
    {
        $engine = new RustParquetEngine(Options::default()->set(Option::INT_96_AS_DATETIME, false)->set(
            Option::ARROW_WRITE_BATCH_SIZE,
            7,
        ));

        static::assertSame(false, $engine->normalize()['options']['INT_96_AS_DATETIME']);
        static::assertSame(7, $engine->normalize()['options']['ARROW_WRITE_BATCH_SIZE']);
        static::assertSame($engine->normalize(), RustParquetEngine::fromArray($engine->normalize())->normalize());
    }

    public function test_a_default_engine_describes_the_default_options(): void
    {
        static::assertSame(Options::default()->normalize(), (new RustParquetEngine())->normalize()['options']);
    }

    public function test_a_configuration_without_options_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Parquet engine configuration must hold an "options" array');

        RustParquetEngine::fromArray([]);
    }
}
