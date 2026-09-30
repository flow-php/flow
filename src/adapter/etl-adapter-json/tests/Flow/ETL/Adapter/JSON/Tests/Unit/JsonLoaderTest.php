<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;

use function array_keys;
use function count;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_list;

use const JSON_PRESERVE_ZERO_FRACTION;
use const JSON_THROW_ON_ERROR;

final class JsonLoaderTest extends FlowTestCase
{
    public function test_setting_content_type_on_path(): void
    {
        $loader = new JsonLoader(path(__DIR__ . '/file.json'));

        static::assertEquals($loader->destination()->getOption(Option::CONTENT_TYPE), ContentType::JSON);
    }

    public function test_every_batch_is_one_append_and_closure_writes_the_bracket(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());
        $schema = schema(int_schema('id'), float_schema('price'), list_schema('days', type_list(type_date())));
        $loader = to_json(path('memory://out.json'), filesystem: $filesystem);

        $loader->load(array_to_rows([[
            'id' => 1,
            'price' => 0.1 + 0.2,
            'days' => [new DateTimeImmutable('2026-01-02')],
        ]], $schema), flow_context());
        $loader->load(array_to_rows([
            ['id' => 2, 'price' => 2.0, 'days' => []],
            ['id' => 3, 'price' => 3.5, 'days' => []],
        ], $schema), flow_context());
        $loader->closure(flow_context());

        static::assertSame(
            '[{"id":1,"price":0.30000000000000004,"days":["2026-01-02"]},{"id":2,"price":2,"days":[]},{"id":3,"price":3.5,"days":[]}]',
            $filesystem->readFrom(path('memory://out.json'))->content(),
        );
        static::assertSame(3, count(array_keys($filesystem->calls, 'append', true)));
    }

    public function test_rows_in_new_lines_with_flags(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_json(path('memory://out.json'), filesystem: $filesystem)
            ->withRowsInNewLines(true)
            ->withFlags(JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION)
            ->withDateFormat('d/m/Y');

        $loader->load(
            array_to_rows(
                [
                    ['price' => 2.0, 'on' => new DateTimeImmutable('2026-01-02')],
                    ['price' => 3.5, 'on' => new DateTimeImmutable('2026-01-03')],
                ],
                schema(float_schema('price'), date_schema('on')),
            ),
            flow_context(),
        );
        $loader->closure(flow_context());

        static::assertSame(
            "[\n{\"price\":2.0,\"on\":\"02\\/01\\/2026\"},\n{\"price\":3.5,\"on\":\"03\\/01\\/2026\"}\n]",
            $filesystem->readFrom(path('memory://out.json'))->content(),
        );
    }

    public function test_every_partition_file_is_closed_with_its_bracket(): void
    {
        $filesystem = memory_filesystem();
        $schema = schema(int_schema('id'), str_schema('group'));
        $loader = to_json(path('memory://out/file.json'), filesystem: $filesystem)->partitionBy(partition_by('group'));

        $loader->load(array_to_rows([
            ['id' => 1, 'group' => 'a'],
            ['id' => 2, 'group' => 'b'],
        ], $schema), flow_context());
        $loader->load(array_to_rows([['id' => 3, 'group' => 'a']], $schema), flow_context());
        $loader->closure(flow_context());

        static::assertSame(
            '[{"id":1},{"id":3}]',
            $filesystem->readFrom(path('memory://out/group=a/file.json'))->content(),
        );
        static::assertSame('[{"id":2}]', $filesystem->readFrom(path('memory://out/group=b/file.json'))->content());
    }

    public function test_a_discarded_loader_starts_over(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_json(path('memory://out.json'), filesystem: $filesystem);

        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context());
        $loader->discard(flow_context());
        $loader->load(array_to_rows([['id' => 2]], schema(int_schema('id'))), flow_context());
        $loader->closure(flow_context());

        static::assertSame('[{"id":2}]', $filesystem->readFrom(path('memory://out.json'))->content());
    }
}
