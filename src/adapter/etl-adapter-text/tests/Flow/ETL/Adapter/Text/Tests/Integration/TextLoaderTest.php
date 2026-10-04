<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use Flow\ETL\Adapter\Text\TextLoader;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;

use function array_keys;
use function array_map;
use function count;
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class TextLoaderTest extends FlowTestCase
{
    public function test_setting_content_type_on_path(): void
    {
        $loader = new TextLoader(path(__DIR__ . '/file.txt'));

        static::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::TEXT);
    }

    public function test_every_batch_is_one_append(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());
        $loader = to_text(path('memory://out.txt'), "\n", $filesystem);

        $loader->load(array_to_rows([
            ['price' => 0.1 + 0.2],
            ['price' => 2.0],
        ], schema(float_schema('price'))), flow_context());
        $loader->load(array_to_rows([['price' => 3.5]], schema(float_schema('price'))), flow_context());
        $loader->closure(flow_context());

        static::assertSame(
            "0.30000000000000004\n2.0\n3.5\n",
            $filesystem->readFrom(path('memory://out.txt'))->content(),
        );
        static::assertSame(2, count(array_keys($filesystem->calls, 'append', true)));
    }

    public function test_two_runs_through_one_loader_write_independent_files(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_text(path('memory://out.txt'), filesystem: $filesystem)->partitionBy(partition_by('group'));
        $read = [];

        foreach (['a' => [1, 2], 'b' => [3]] as $group => $ids) {
            $loader->load(
                array_to_rows(
                    array_map(static fn(int $id): array => ['text' => (string) $id, 'group' => $group], $ids),
                    schema(str_schema('text'), str_schema('group')),
                ),
                flow_context(),
            );
            $loader->closure(flow_context());
            $read[] = $filesystem->readFrom(path('memory://group=' . $group . '/out.txt'))->content();
        }

        static::assertSame(["1\n2\n", "3\n"], $read);
    }
}
