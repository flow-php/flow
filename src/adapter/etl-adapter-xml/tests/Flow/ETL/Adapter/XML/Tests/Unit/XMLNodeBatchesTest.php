<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use DOMDocument;
use Flow\ETL\Adapter\XML\XMLNodeBatches;
use Flow\ETL\Adapter\XML\XMLNodes;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Column\Physical\XmlDocumentPhysical;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Definition\XMLDefinition;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Stream\StringSourceStream;

use function array_map;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_instance_of;
use function iterator_to_array;

final class XMLNodeBatchesTest extends FlowTestCase
{
    public function test_documents_are_rows_of_utf8_node_documents_in_batches(): void
    {
        $batches = iterator_to_array(
            (new XMLNodeBatches(new XMLNodes('rows/row'), 2, 8192))->documents(
                new StringSourceStream(path('memory://a.xml'), '<rows><row>ż</row><row>2</row><row>3</row></rows>'),
                schema(xml_schema('node'), str_schema('other', nullable: true)),
                new PhpBackend(),
            ),
            false,
        );

        static::assertSame([2, 1], array_map(static fn(Rows $rows): int => $rows->count(), $batches));

        $first = type_instance_of(DOMDocument::class)->assert($batches[0]->column('node')->value(0));

        static::assertSame('UTF-8', $first->encoding);
        static::assertSame('<row>ż</row>', $first->saveXML($first->documentElement));
        static::assertSame([null, null], $batches[0]->column('other')->values());
    }

    public function test_physicals_are_an_xml_column_of_node_document_bytes_in_batches(): void
    {
        $node = xml_schema('node');
        static::assertInstanceOf(XMLDefinition::class, $node);

        $batches = iterator_to_array(
            (new XMLNodeBatches(new XMLNodes('rows/row'), 2, 8192))->physicals(
                new StringSourceStream(path('memory://a.xml'), '<rows><row a="ż"/><row>2</row><row>3</row></rows>'),
                $node,
                schema($node),
                new PhpBackend(),
            ),
            false,
        );

        static::assertSame([2, 1], array_map(static fn(Rows $rows): int => $rows->count(), $batches));
        static::assertSame(
            [
                XmlDocumentPhysical::DECLARATION . "<row a=\"ż\"/>\n",
                XmlDocumentPhysical::DECLARATION . "<row>2</row>\n",
            ],
            $batches[0]->column('node')->physicals(),
        );
    }

    public function test_strings_are_rows_of_node_texts_in_batches(): void
    {
        $batches = iterator_to_array(
            (new XMLNodeBatches(new XMLNodes('rows/row'), 2, 8192))->strings(
                new StringSourceStream(path('memory://a.xml'), '<rows><row>ż</row><row>2</row><row>3</row></rows>'),
                schema(str_schema('node')),
                new PhpBackend(),
            ),
            false,
        );

        static::assertSame(
            [[['node' => '<row>ż</row>'], ['node' => '<row>2</row>']], [['node' => '<row>3</row>']]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $batches),
        );
    }
}
