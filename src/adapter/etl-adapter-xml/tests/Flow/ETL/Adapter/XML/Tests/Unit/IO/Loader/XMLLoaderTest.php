<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit\IO\Loader;

use Flow\ETL\Adapter\XML\IO\Loader\XMLLoader;
use Flow\ETL\Adapter\XML\IO\Loader\XMLNode;
use Flow\ETL\Adapter\XML\IO\Loader\XMLWriter;
use Flow\ETL\Adapter\XML\IO\Loader\XMLWriter\DOMDocumentWriter;
use Flow\ETL\Adapter\XML\IO\Loader\XMLWriter\StringXMLWriter;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
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
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_instance_of;

final class XMLLoaderTest extends FlowTestCase
{
    public function test_setting_content_type_on_path(): void
    {
        static::assertEquals(
            (new XMLLoader(path(__DIR__ . '/file.csv'), new DOMDocumentWriter()))
                ->destination()
                ->getOption(Option::CONTENT_TYPE),
            ContentType::XML,
        );
    }

    public function test_every_batch_is_one_append_and_closure_closes_the_root(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());
        $schema = schema(int_schema('_id'), str_schema('name'));
        $loader = to_xml(path('memory://out.xml'), filesystem: $filesystem);

        $loader->load(array_to_rows([['_id' => 1, 'name' => 'a']], $schema), flow_context()->backend());
        $loader->load(array_to_rows([
            ['_id' => 2, 'name' => 'b'],
            ['_id' => 3, 'name' => 'c'],
        ], $schema), flow_context()->backend());
        $loader->closure();

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
        ], $schema), flow_context()->backend());
        $loader->load(array_to_rows([['id' => 3, 'group' => 'a']], $schema), flow_context()->backend());
        $loader->closure();

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
        // the second run writes beside the first one's partition, which ExceptionIfExists refuses
        $loader = to_xml(path('memory://out.xml'), filesystem: $filesystem)
            ->partitionBy(partition_by('group'))
            ->saveMode(SaveMode::Append);
        $read = [];

        foreach (['a' => [1, 2], 'b' => [3]] as $group => $ids) {
            $loader->load(
                array_to_rows(
                    array_map(static fn(int $id): array => ['id' => $id, 'group' => $group], $ids),
                    schema(int_schema('id'), str_schema('group')),
                ),
                flow_context()->backend(),
            );
            $loader->closure();
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

    public function test_describe_round_trips(): void
    {
        $scratch = ReaderRoundTrip::scratch('xml-describe');
        $file = $scratch->suffix('/rows.xml');
        $loader = to_xml($file, root_element_name: 'items', row_element_name: 'item')
            ->saveMode(SaveMode::Overwrite)
            ->withXMLAttributes(['version' => '1.0', 'encoding' => 'UTF-8', 'standalone' => 'yes']);
        $rebuilt = DescribeRoundTrip::rebuilt($loader);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );

        static::assertSame(
            StepRoundTrip::written($loader, $rows, flow_context(), $file),
            StepRoundTrip::written($rebuilt, $rows, flow_context(), $file),
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_a_partitioned_loader_describes_its_partitioning(): void
    {
        $scratch = ReaderRoundTrip::scratch('xml-describe-partitioned');
        static::assertSame(
            ['by' => ['name'], 'write_columns' => true],
            DescribeRoundTrip::rebuilt(
                to_xml($scratch->suffix('/{name}/rows.xml'), root_element_name: 'items', row_element_name: 'item')
                    ->saveMode(SaveMode::Overwrite)
                    ->withXMLAttributes(['version' => '1.0', 'encoding' => 'UTF-8', 'standalone' => 'yes'])
                    ->partitionBy(partition_by('name')->writeColumns()),
            )->describe()?->data['partitioning'],
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_a_live_filesystem_does_not_describe(): void
    {
        static::assertNull(to_xml(path('memory://rows.xml'), filesystem: memory_filesystem())->describe());
    }

    public function test_a_custom_xml_writer_does_not_describe(): void
    {
        static::assertNull(
            (new XMLLoader(
                ($scratch = ReaderRoundTrip::scratch('xml-custom'))->suffix('/rows.xml'),
                new class implements XMLWriter {
                    public function attributes(string $name, array $values): array
                    {
                        return [];
                    }

                    public function elements(string $name, array $values): array
                    {
                        return [];
                    }

                    public function write(XMLNode $node): string
                    {
                        return '';
                    }
                },
            ))->describe(),
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_a_string_writer_round_trips(): void
    {
        $scratch = ReaderRoundTrip::scratch('xml-string-writer');
        $file = $scratch->suffix('/rows.xml');
        $loader = new XMLLoader($file, new StringXMLWriter());
        $rebuilt = DescribeRoundTrip::rebuilt($loader);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );

        static::assertSame(
            StringXMLWriter::class,
            type_instance_of(Description::class)->assert($rebuilt->describe()?->data['xml_writer'])->class,
        );
        static::assertSame(
            StepRoundTrip::written($loader, $rows, flow_context(), $file),
            StepRoundTrip::written($rebuilt, $rows, flow_context(), $file),
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_in_task_copies_every_setting_and_leaves_the_original(): void
    {
        $scratch = ReaderRoundTrip::scratch('in-task');
        $loader = to_xml($scratch->suffix('/rows.out'), root_element_name: 'items')
            ->saveMode(SaveMode::Overwrite)
            ->partitionBy(partition_by('name'))
            ->withXMLAttributes(['version' => '1.1']);
        $copy = $loader->inTask(new Part(3, 4, '1a2b3c4d5e6f7a8b', false));

        static::assertSame($loader->destination()->uri(), $copy->destination()->uri());
        static::assertSame($scratch->suffix('/rows.out')->uri(), $loader->destination()->uri());
        static::assertEquals($loader->partitioning(), $copy->partitioning());

        $original = $loader->describe()->data ?? [];
        $copied = $copy->describe()->data ?? [];
        static::assertNull($original['task']);
        static::assertSame(
            ['index' => 3, 'splits' => 4, 'run' => '1a2b3c4d5e6f7a8b', 'append' => false],
            $copied['task'],
        );
        static::assertEquals($copied, DescribeRoundTrip::rebuilt($copy)->describe()?->data);
        unset($original['task'], $copied['task']);

        static::assertEquals($original, $copied);

        native_local_filesystem()->rm($scratch);
    }

    public function test_commit_applies_the_save_mode_on_the_loader_filesystem(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://out/rows.xml'))->append('old')->close();
        $loader = to_xml(path('memory://out/rows.xml'), filesystem: $filesystem);

        static::assertNull($loader->saveMode(SaveMode::Ignore)->commit('r1', 1));
        static::assertInstanceOf(FileCommit::class, $loader->saveMode(SaveMode::Overwrite)->commit('r1', 1));

        $this->expectException(RuntimeException::class);

        $loader->saveMode(SaveMode::ExceptionIfExists)->commit('r1', 1);
    }

    public function test_partitioning_returns_what_partition_by_set(): void
    {
        $loader = to_xml(path('file:///tmp/rows.xml'), filesystem: native_local_filesystem());

        static::assertEquals(Partitioning::none(), $loader->partitioning());
        static::assertEquals(
            partition_by('name')->writeColumns(),
            $loader->partitionBy(partition_by('name')->writeColumns())->partitioning(),
        );
    }

    public function test_the_loader_declares_its_writers_xml_extensions(): void
    {
        static::assertSame(
            ['dom', 'xml', 'xmlreader', 'xmlwriter'],
            to_xml(path(__DIR__ . '/var/rows.xml'))->describe()?->extensions(),
        );
    }
}
