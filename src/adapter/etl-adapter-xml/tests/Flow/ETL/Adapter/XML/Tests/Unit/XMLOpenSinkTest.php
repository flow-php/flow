<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use Flow\ETL\Adapter\XML\XMLEncoder;
use Flow\ETL\Adapter\XML\XMLOpenSink;
use Flow\ETL\Adapter\XML\XMLWriter\StringXMLWriter;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class XMLOpenSinkTest extends FlowTestCase
{
    public function test_the_head_precedes_the_first_write_only_and_close_ends_the_root(): void
    {
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(path('memory://out.xml'));
        $sink = new XMLOpenSink($stream, new XMLEncoder(new StringXMLWriter()), "<?xml?>\n<root>\n", 'root');

        $sink->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $sink->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
        $sink->close();
        $stream->close();

        static::assertSame(
            "<?xml?>\n<root>\n<row><id>1</id></row>\n<row><id>2</id></row>\n</root>",
            $filesystem->readFrom(path('memory://out.xml'))->content(),
        );
    }

    public function test_a_sink_that_wrote_nothing_closes_without_a_root_end(): void
    {
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(path('memory://out.xml'));

        (new XMLOpenSink($stream, new XMLEncoder(new StringXMLWriter()), "<?xml?>\n<root>\n", 'root'))->close();
        $stream->close();

        static::assertSame('', $filesystem->readFrom(path('memory://out.xml'))->content());
    }
}
