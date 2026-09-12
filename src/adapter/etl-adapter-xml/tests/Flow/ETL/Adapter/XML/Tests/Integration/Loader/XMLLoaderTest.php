<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration\Loader;

use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Exception\RuntimeException as FilesystemRuntimeException;
use Flow\Filesystem\Tests\Double\FailingCloseFilesystem;

use function file_exists;
use function file_get_contents;
use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\Adapter\XML\to_xml;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_transformation;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class XMLLoaderTest extends FlowIntegrationTestCase
{
    public function test_partitioning_xml_file(): void
    {
        df()
            ->read(from_array(
                $dataset = [
                    ['id' => 1, 'color' => 'red', 'size' => 'small'],
                    ['id' => 2, 'color' => 'blue', 'size' => 'medium'],
                    ['id' => 3, 'color' => 'green', 'size' => 'large'],
                    ['id' => 4, 'color' => 'yellow', 'size' => 'small'],
                    ['id' => 5, 'color' => 'black', 'size' => 'medium'],
                    ['id' => 6, 'color' => 'white', 'size' => 'large'],
                    ['id' => 7, 'color' => 'red', 'size' => 'small'],
                    ['id' => 8, 'color' => 'blue', 'size' => 'medium'],
                    ['id' => 9, 'color' => 'green', 'size' => 'large'],
                    ['id' => 10, 'color' => 'yellow', 'size' => 'small'],
                    ['id' => 11, 'color' => 'black', 'size' => 'medium'],
                    ['id' => 12, 'color' => 'white', 'size' => 'large'],
                ],
            ))
            ->batchSize(1)
            ->write(
                to_xml(__DIR__ . '/var/test_partitioning_xml_file/products.xml')
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('size', 'color')),
            )
            ->run();

        static::assertEquals(
            $dataset,
            df()
                ->read(from_xml(__DIR__ . '/var/test_partitioning_xml_file/**/*.xml')->withXMLNodePath('rows/row'))
                ->withEntry('id', ref('node')->xpath('id')->domElementValue()->cast('int'))
                // color and size are no longer in the body; the read takes them from the path
                ->drop('node')
                ->sortBy([ref('id')->asc()])
                ->fetch()
                ->toArray(),
        );
    }

    public function test_transformation_loader_writes_all_batches_to_xml(): void
    {
        df()
            ->read(from_sequence_number('id', 1, 12))
            ->withEntry('name', lit('dropped by the transformation'))
            ->batchSize(4)
            ->write(to_transformation(
                select('id'),
                to_xml($path = $this->cacheDir->suffix('transformation.xml'))->saveMode(overwrite()),
            ))
            ->run();

        $content = file_get_contents($path->path());

        if ($content === false) {
            static::fail('Failed to read file content');
        }

        static::assertStringNotContainsString('dropped by the transformation', $content);
        static::assertCount(12, df()->read(from_xml($path, 'rows/row'))->fetch());
    }

    public function test_writing_empty_rows(): void
    {
        df()
            ->read(from_array([]))
            ->write(to_xml($path = $this->cacheDir->suffix('test_xml_loader.xml')))
            ->run();

        static::assertFalse(file_exists($path->path()));
    }

    public function test_writing_xml(): void
    {
        df()
            ->read(new FakeExtractor(100))
            ->write(to_xml($path = $this->cacheDir->suffix('test_xml_loader.xml'))->saveMode(overwrite()))
            ->run();

        static::assertEquals(100, df()->read(from_xml($path, 'rows/row'))->count());
    }

    public function test_writing_xml_with_attributes(): void
    {
        df()
            ->read(from_array([
                [
                    '_id' => 1,
                    'name' => 'John',
                    'address' => ['_id' => 1, 'city' => 'New York', 'street' => '5th Avenue'],
                ],
                [
                    '_id' => 2,
                    'name' => 'Jane',
                    'address' => ['_id' => 2, 'city' => 'Los Angeles', 'street' => 'Hollywood Boulevard'],
                ],
            ]))
            ->write(to_xml($path = $this->cacheDir->suffix('test_xml_loader.xml')))
            ->run();

        $content = file_get_contents($path->path());
        static::assertNotFalse($content);
        static::assertXmlStringEqualsXmlString(<<<'XML'
            <?xml version="1.0" encoding="UTF-8"?>
            <rows>
            <row id="1"><name>John</name><address id="1"><city>New York</city><street>5th Avenue</street></address></row>
            <row id="2"><name>Jane</name><address id="2"><city>Los Angeles</city><street>Hollywood Boulevard</street></address></row>
            </rows>
            XML, $content);
    }

    public function test_a_close_that_fails_during_closure_leaves_no_file(): void
    {
        $memory = memory_filesystem();

        try {
            df()
                ->read(from_array([['p' => 'a', 't' => 'x'], ['p' => 'b', 't' => 'y'], ['p' => 'c', 't' => 'z']]))
                ->write(to_xml(
                    path('memory://var/staged/file.xml'),
                    filesystem: new FailingCloseFilesystem($memory, failingStreams: 2),
                )->partitionBy(partition_by('p')))
                ->run();
            static::fail('the run was expected to throw');
        } catch (FilesystemRuntimeException $failure) {
            static::assertSame('Closing "memory://var/staged/p=a/file.xml" failed', $failure->getMessage());
        }

        static::assertSame([], iterator_to_array($memory->list(path('memory://var/staged/**/*')), false));
    }
}
