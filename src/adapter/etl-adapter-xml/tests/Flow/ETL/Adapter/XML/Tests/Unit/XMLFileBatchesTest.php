<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use Flow\ETL\Adapter\XML\XMLFileBatches;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class XMLFileBatchesTest extends FlowTestCase
{
    public function test_matched_elements_land_in_the_node_column_and_no_offset_is_consumed(): void
    {
        $filesystem = new CountingFilesystem(MemoryFiles::with([
            'memory://a.xml' => '<root><row><id>1</id></row><row><id>2</id></row></root>',
        ]));
        $file = (new XMLFileBatches($filesystem, 'root/row', 8192))->batches(
            new SourceFile(path('memory://a.xml')),
            schema(str_schema('node')),
            10,
            new PhpBackend(),
            new ReadWindow(),
        );

        static::assertSame(
            [[['node' => '<row><id>1</id></row>'], ['node' => '<row><id>2</id></row>']]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), iterator_to_array($file, false)),
        );
        static::assertSame(0, $file->getReturn());
        static::assertSame($filesystem->readFromCalls, $filesystem->closedStreams());
    }

    public function test_a_column_the_body_declares_beside_node_is_padded_with_nulls(): void
    {
        $file = (new XMLFileBatches(
            MemoryFiles::with(['memory://a.xml' => '<root><row>1</row></root>']),
            'root/row',
            8192,
        ))->batches(
            new SourceFile(path('memory://a.xml')),
            schema(str_schema('node'), str_schema('note', nullable: true)),
            10,
            new PhpBackend(),
            new ReadWindow(),
        );

        static::assertSame(
            [[['node' => '<row>1</row>', 'note' => null]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), iterator_to_array($file, false)),
        );
    }
}
