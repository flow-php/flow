<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\JSON\JsonFraming;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;

use function array_keys;
use function count;
use function Flow\ETL\Adapter\JSON\to_json_lines;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

use const JSON_PRETTY_PRINT;
use const JSON_UNESCAPED_SLASHES;

final class JsonLinesLoaderTest extends FlowTestCase
{
    public function test_setting_content_type_on_path(): void
    {
        $loader = new JsonLoader(path(__DIR__ . '/file.jsonl'), framing: JsonFraming::Lines);

        static::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::JSON);
    }

    public function test_every_batch_is_one_append_and_closure_ends_the_last_line(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());
        $schema = schema(int_schema('id'), float_schema('price'));
        $loader = to_json_lines(path('memory://out.jsonl'), filesystem: $filesystem);

        $loader->load(array_to_rows([['id' => 1, 'price' => 0.1 + 0.2]], $schema), flow_context());
        $loader->load(array_to_rows([
            ['id' => 2, 'price' => 2.0],
            ['id' => 3, 'price' => 3.5],
        ], $schema), flow_context());
        $loader->closure(flow_context());

        static::assertSame(
            "{\"id\":1,\"price\":0.30000000000000004}\n{\"id\":2,\"price\":2}\n{\"id\":3,\"price\":3.5}\n",
            $filesystem->readFrom(path('memory://out.jsonl'))->content(),
        );
        static::assertSame(3, count(array_keys($filesystem->calls, 'append', true)));
    }

    public function test_pretty_print_is_ignored_and_the_other_options_apply(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_json_lines(path('memory://out.jsonl'), filesystem: $filesystem)
            ->withFlags(JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)
            ->withDateTimeFormat('d/m/Y H:i');

        $loader->load(array_to_rows([[
            'at' => new DateTimeImmutable('2026-01-02 03:04:05 UTC'),
        ]], schema(datetime_schema('at'))), flow_context());
        $loader->closure(flow_context());

        static::assertSame(
            "{\"at\":\"02/01/2026 03:04\"}\n",
            $filesystem->readFrom(path('memory://out.jsonl'))->content(),
        );
    }

    public function test_rows_in_new_lines_is_refused_for_json_lines(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('withRowsInNewLines() applies to to_json(), not to_json_lines()');

        to_json_lines(path('memory://out.jsonl'), filesystem: memory_filesystem())->withRowsInNewLines(true);
    }

    public function test_every_partition_file_ends_with_a_new_line(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'), str_schema('group'));
        $loader = to_json_lines(path('memory://out/file.jsonl'), filesystem: $filesystem)->partitionBy(partition_by(
            'group',
        ));

        $loader->load(array_to_rows([
            ['id' => 1, 'group' => 'a'],
            ['id' => 2, 'group' => 'b'],
        ], $schema), flow_context());
        $loader->load(array_to_rows([['id' => 3, 'group' => 'a']], $schema), flow_context());
        $loader->closure(flow_context());

        static::assertSame(
            "{\"id\":1}\n{\"id\":3}\n",
            $filesystem->readFrom(path('memory://out/group=a/file.jsonl'))->content(),
        );
        static::assertSame("{\"id\":2}\n", $filesystem->readFrom(path('memory://out/group=b/file.jsonl'))->content());
    }

    public function test_a_discarded_loader_starts_over(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_json_lines(path('memory://out.jsonl'), filesystem: $filesystem);

        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context());
        $loader->discard(flow_context());
        $loader->load(array_to_rows([['id' => 2]], schema(int_schema('id'))), flow_context());
        $loader->closure(flow_context());

        static::assertSame("{\"id\":2}\n", $filesystem->readFrom(path('memory://out.jsonl'))->content());
    }
}
