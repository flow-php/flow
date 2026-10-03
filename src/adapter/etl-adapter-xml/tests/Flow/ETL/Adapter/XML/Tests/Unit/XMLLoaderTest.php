<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use Flow\ETL\Adapter\XML\Loader\XMLLoader;
use Flow\ETL\Adapter\XML\XMLWriter\DOMDocumentWriter;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;

use function array_keys;
use function array_map;
use function count;
use function Flow\ETL\Adapter\XML\to_xml;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class XMLLoaderTest extends FlowTestCase
{
    public function test_setting_content_type_on_path(): void
    {
        $loader = new XMLLoader(path(__DIR__ . '/file.csv'), new DOMDocumentWriter());

        static::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::XML);
    }

    public function test_every_batch_is_one_append_and_closure_closes_the_root(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());
        $schema = schema(int_schema('_id'), str_schema('name'));
        $loader = to_xml(path('memory://out.xml'), filesystem: $filesystem);

        $loader->load(array_to_rows([['_id' => 1, 'name' => 'a']], $schema), flow_context());
        $loader->load(array_to_rows([
            ['_id' => 2, 'name' => 'b'],
            ['_id' => 3, 'name' => 'c'],
        ], $schema), flow_context());
        $loader->closure(flow_context());

        static::assertSame(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rows>\n"
            . "<row id=\"1\"><name>a</name></row>\n<row id=\"2\"><name>b</name></row>\n<row id=\"3\"><name>c</name></row>\n"
            . '</rows>',
            $filesystem->readFrom(path('memory://out.xml'))->content(),
        );
        static::assertSame(3, count(array_keys($filesystem->calls, 'append', true)));
    }

    public function test_every_partition_file_has_its_own_root(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'), str_schema('group'));
        $loader = to_xml(path('memory://out/file.xml'), filesystem: $filesystem)->partitionBy(partition_by('group'));

        $loader->load(array_to_rows([
            ['id' => 1, 'group' => 'a'],
            ['id' => 2, 'group' => 'b'],
        ], $schema), flow_context());
        $loader->load(array_to_rows([['id' => 3, 'group' => 'a']], $schema), flow_context());
        $loader->closure(flow_context());

        static::assertSame(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rows>\n<row><id>1</id></row>\n<row><id>3</id></row>\n</rows>",
            $filesystem->readFrom(path('memory://out/group=a/file.xml'))->content(),
        );
        static::assertSame(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rows>\n<row><id>2</id></row>\n</rows>",
            $filesystem->readFrom(path('memory://out/group=b/file.xml'))->content(),
        );
    }

    public function test_two_runs_through_one_loader_write_independent_files(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_xml(path('memory://out.xml'), filesystem: $filesystem)->partitionBy(partition_by('group'));
        $read = [];

        foreach (['a' => [1, 2], 'b' => [3]] as $group => $ids) {
            $loader->load(
                array_to_rows(
                    array_map(static fn(int $id): array => ['id' => $id, 'group' => $group], $ids),
                    schema(int_schema('id'), str_schema('group')),
                ),
                flow_context(),
            );
            $loader->closure(flow_context());
            $read[] = $filesystem->readFrom(path('memory://group=' . $group . '/out.xml'))->content();
        }

        static::assertSame(
            [
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rows>\n<row><id>1</id></row>\n<row><id>2</id></row>\n</rows>",
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<rows>\n<row><id>3</id></row>\n</rows>",
            ],
            $read,
        );
    }
}
