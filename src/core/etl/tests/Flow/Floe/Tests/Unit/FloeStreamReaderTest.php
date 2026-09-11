<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\FloeStreamReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\Tests\Double\ClosingSpySourceStream;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FloeStreamReaderTest extends TestCase
{
    public function test_close_closes_the_source_stream(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://close.floe');

        $data = rows(schema(int_schema('id')), row(['id' => 1]));
        $writer = new FloeWriter($filesystem, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $source = new ClosingSpySourceStream($filesystem->readFrom($path));

        (new FloeStreamReader($source, new NoopCodec(), 65_536))->close();

        static::assertSame(1, $source->closeCount);
    }
}
