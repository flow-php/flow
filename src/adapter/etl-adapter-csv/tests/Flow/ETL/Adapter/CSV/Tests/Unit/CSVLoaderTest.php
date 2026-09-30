<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\CSV\CSVLoader;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;

use function array_keys;
use function count;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class CSVLoaderTest extends FlowTestCase
{
    public function test_setting_content_type_on_path(): void
    {
        $loader = new CSVLoader(path(__DIR__ . '/file.csv'));

        static::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::CSV);
    }

    public function test_every_batch_is_one_append_and_the_header_comes_with_the_first(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());
        $schema = schema(int_schema('id'), str_schema('name'), float_schema('price'), bool_schema('active'));
        $loader = to_csv(path('memory://out.csv'), filesystem: $filesystem)->withNewLineSeparator("\n");

        $loader->load(array_to_rows([[
            'id' => 1,
            'name' => 'a b',
            'price' => 0.1 + 0.2,
            'active' => true,
        ]], $schema), flow_context());
        $loader->load(array_to_rows([
            ['id' => 2, 'name' => 'c', 'price' => 2.0, 'active' => false],
            ['id' => 3, 'name' => 'd', 'price' => 3.5, 'active' => true],
        ], $schema), flow_context());
        $loader->closure(flow_context());

        static::assertSame(
            "id,name,price,active\n1,\"a b\",0.30000000000000004,true\n2,c,2.0,false\n3,d,3.5,true\n",
            $filesystem->readFrom(path('memory://out.csv'))->content(),
        );
        static::assertSame(2, count(array_keys($filesystem->calls, 'append', true)));
    }

    public function test_every_partition_file_gets_its_own_header(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'), str_schema('group'));
        $loader = to_csv(path('memory://out/file.csv'), filesystem: $filesystem)
            ->withNewLineSeparator("\n")
            ->partitionBy(partition_by('group'));

        $loader->load(array_to_rows([
            ['id' => 1, 'group' => 'a'],
            ['id' => 2, 'group' => 'b'],
        ], $schema), flow_context());
        $loader->load(array_to_rows([['id' => 3, 'group' => 'a']], $schema), flow_context());
        $loader->closure(flow_context());

        static::assertSame("id\n1\n3\n", $filesystem->readFrom(path('memory://out/group=a/file.csv'))->content());
        static::assertSame("id\n2\n", $filesystem->readFrom(path('memory://out/group=b/file.csv'))->content());
    }

    public function test_the_loader_writes_with_its_options(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_csv(path('memory://out.csv'), filesystem: $filesystem)
            ->withHeader(false)
            ->withSeparator(';')
            ->withEnclosure("'")
            ->withEscape('')
            ->withNewLineSeparator("\r\n")
            ->withDateTimeFormat('Y-m-d H:i');

        $loader->load(
            array_to_rows(
                [['name' => "it's;", 'at' => new DateTimeImmutable('2026-01-02 03:04:05 UTC')]],
                schema(str_schema('name'), datetime_schema('at')),
            ),
            flow_context(),
        );
        $loader->closure(flow_context());

        static::assertSame(
            "'it''s;';'2026-01-02 03:04'\r\n",
            $filesystem->readFrom(path('memory://out.csv'))->content(),
        );
    }

    public function test_a_loader_used_again_after_closure_writes_the_header_again(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_csv(path('memory://out.csv'), filesystem: $filesystem)->withNewLineSeparator("\n");

        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context());
        $loader->closure(flow_context());
        $filesystem->rm(path('memory://out.csv'));
        $loader->load(array_to_rows([['id' => 2]], schema(int_schema('id'))), flow_context());
        $loader->closure(flow_context());

        static::assertSame("id\n2\n", $filesystem->readFrom(path('memory://out.csv'))->content());
    }

    public function test_a_discarded_loader_starts_over(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_csv(path('memory://out.csv'), filesystem: $filesystem)->withNewLineSeparator("\n");

        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context());
        $loader->discard(flow_context());
        $loader->load(array_to_rows([['id' => 2]], schema(int_schema('id'))), flow_context());
        $loader->closure(flow_context());

        static::assertSame("id\n2\n", $filesystem->readFrom(path('memory://out.csv'))->content());
    }
}
