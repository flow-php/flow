<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Floe\FloeFileSinks;
use Flow\Floe\Options;
use Flow\Floe\Tests\Context\FloeFilesContext;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FloeFileSinkTest extends FlowTestCase
{
    public function test_a_sink_closed_before_its_first_write_writes_nothing(): void
    {
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(path('memory://a.floe'));

        (new FloeFileSinks($filesystem, null, null, new Options()))
            ->open($stream, new PhpBackend())
            ->close();
        $stream->close();

        static::assertSame('', $filesystem->readFrom(path('memory://a.floe'))->content());
    }

    public function test_the_first_write_opens_the_writer_and_every_write_lands_in_the_file(): void
    {
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(path('memory://a.floe'));
        $sink = (new FloeFileSinks($filesystem, null, null, new Options()))->open($stream, new PhpBackend());

        $sink->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $sink->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
        $sink->close();
        $stream->close();

        $rows = [];

        foreach (FloeFilesContext::phpReader($filesystem)->read(path('memory://a.floe'))->rows() as $batch) {
            $rows = [...$rows, ...$batch->toArray()];
        }

        static::assertSame([['id' => 1], ['id' => 2]], $rows);
    }
}
