<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit\IO\Loader;

use DateTimeImmutable;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Adapter\JSON\IO\Loader\JsonLoader;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ReaderRoundTrip;
use Flow\ETL\Tests\Context\StepRoundTrip;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use Flow\Filesystem\Stream\StringDestinationStream;

use function array_keys;
use function array_map;
use function count;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\Adapter\JSON\to_json_lines;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_list;

use const JSON_HEX_TAG;
use const JSON_PRESERVE_ZERO_FRACTION;
use const JSON_PRETTY_PRINT;
use const JSON_THROW_ON_ERROR;
use const JSON_UNESCAPED_SLASHES;

final class JsonLoaderTest extends FlowTestCase
{
    public function test_setting_content_type_on_path(): void
    {
        static::assertEquals(
            (new JsonLoader(path(__DIR__ . '/file.json')))
                ->destination()
                ->getOption(Option::CONTENT_TYPE),
            ContentType::JSON,
        );
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
        ]], $schema), flow_context()->backend());
        $loader->load(array_to_rows([
            ['id' => 2, 'price' => 2.0, 'days' => []],
            ['id' => 3, 'price' => 3.5, 'days' => []],
        ], $schema), flow_context()->backend());
        $loader->closure();

        static::assertSame(
            '[{"id":1,"price":0.30000000000000004,"days":["2026-01-02"]},{"id":2,"price":2,"days":[]},{"id":3,"price":3.5,"days":[]}]',
            $filesystem->readFrom(path('memory://out.json'))->content(),
        );
        static::assertSame(3, count(array_keys($filesystem->calls, 'append', true)));
    }

    public function test_a_batch_without_rows_writes_no_file(): void
    {
        $filesystem = memory_filesystem();
        $loader = to_json(path('memory://out.json'), filesystem: $filesystem);

        $loader->load(array_to_rows([], schema(int_schema('id'))), flow_context()->backend());
        $loader->closure();

        static::assertNull($filesystem->status(path('memory://out.json')));
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
            flow_context()->backend(),
        );
        $loader->closure();

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
        ], $schema), flow_context()->backend());
        $loader->load(array_to_rows([['id' => 3, 'group' => 'a']], $schema), flow_context()->backend());
        $loader->closure();

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

        $loader->load(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend());
        $loader->discard();
        $loader->load(array_to_rows([['id' => 2]], schema(int_schema('id'))), flow_context()->backend());
        $loader->closure();

        static::assertSame('[{"id":2}]', $filesystem->readFrom(path('memory://out.json'))->content());
    }

    public function test_two_runs_through_one_loader_write_independent_files(): void
    {
        $filesystem = memory_filesystem();
        // the second run writes beside the first one's partition, which ExceptionIfExists refuses
        $loader = to_json(path('memory://out.json'), filesystem: $filesystem)
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
            $read[] = $filesystem->readFrom(path('memory://group=' . $group . '/out.json'))->content();
        }

        static::assertSame(['[{"id":1},{"id":2}]', '[{"id":3}]'], $read);
    }

    public function test_describe_round_trips(): void
    {
        $scratch = ReaderRoundTrip::scratch('json-describe');
        $file = $scratch->suffix('/rows.json');
        $loader = to_json($file, flags: JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT, put_rows_in_new_lines: true)
            ->saveMode(SaveMode::Overwrite)
            ->withDateFormat('d/m/Y');
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
        $scratch = ReaderRoundTrip::scratch('json-describe-partitioned');
        static::assertSame(
            ['by' => ['name'], 'write_columns' => true],
            DescribeRoundTrip::rebuilt(
                to_json(
                    $scratch->suffix('/{name}/rows.json'),
                    flags: JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT,
                    put_rows_in_new_lines: true,
                )
                    ->saveMode(SaveMode::Overwrite)
                    ->withDateFormat('d/m/Y')
                    ->partitionBy(partition_by('name')->writeColumns()),
            )->describe()?->data['partitioning'],
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_a_live_filesystem_does_not_describe(): void
    {
        static::assertNull(to_json(path('memory://rows.json'), filesystem: memory_filesystem())->describe());
    }

    public function test_a_json_lines_loader_round_trips_its_framing(): void
    {
        $scratch = ReaderRoundTrip::scratch('jsonl-describe');
        $file = $scratch->suffix('/rows.jsonl');
        $loader = to_json_lines($file);
        $rebuilt = DescribeRoundTrip::rebuilt($loader);
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b'], ['id' => 3, 'name' => 'c']],
            schema(int_schema('id'), str_schema('name')),
        );

        static::assertSame('Lines', $rebuilt->describe()?->data['framing']);
        static::assertSame(
            StepRoundTrip::written($loader, $rows, flow_context(), $file),
            StepRoundTrip::written($rebuilt, $rows, flow_context(), $file),
        );

        native_local_filesystem()->rm($scratch);
    }

    public function test_in_task_copies_every_setting_and_leaves_the_original(): void
    {
        $scratch = ReaderRoundTrip::scratch('in-task');
        $loader = to_json($scratch->suffix('/rows.out'), put_rows_in_new_lines: true)
            ->saveMode(SaveMode::Overwrite)
            ->partitionBy(partition_by('name'))
            ->withDateFormat('d/m/Y');
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
        $filesystem->writeTo(path('memory://out/rows.json'))->append('old')->close();
        $loader = to_json(path('memory://out/rows.json'), filesystem: $filesystem);

        static::assertNull($loader->saveMode(SaveMode::Ignore)->commit('r1', 1));
        static::assertInstanceOf(FileCommit::class, $loader->saveMode(SaveMode::Overwrite)->commit('r1', 1));

        $this->expectException(RuntimeException::class);

        $loader->saveMode(SaveMode::ExceptionIfExists)->commit('r1', 1);
    }

    public function test_partitioning_returns_what_partition_by_set(): void
    {
        $loader = to_json(path('file:///tmp/rows.json'), filesystem: native_local_filesystem());

        static::assertEquals(Partitioning::none(), $loader->partitioning());
        static::assertEquals(
            partition_by('name')->writeColumns(),
            $loader->partitionBy(partition_by('name')->writeColumns())->partitioning(),
        );
    }

    public function test_the_loader_declares_json(): void
    {
        static::assertSame(['json'], to_json(path(__DIR__ . '/var/rows.json'))->describe()?->extensions());
    }

    public function test_a_writer_writes_with_the_options_of_the_loader(): void
    {
        $stream = new StringDestinationStream(path('memory://out.json'));
        $schema = schema(datetime_schema('at'), date_schema('on'));
        $writer = (new JsonLoader(path('memory://out.json'), memory_filesystem()))
            ->withFlags(JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES)
            ->withDateTimeFormat('d/m/Y H:i')
            ->withDateFormat('Y/m/d')
            ->writer($stream, $schema, new PhpBackend());

        $writer->write(array_to_rows([[
            'at' => new DateTimeImmutable('2023-10-01 12:02:01 UTC'),
            'on' => new DateTimeImmutable('2023-10-01'),
        ]], $schema));
        $writer->close();

        static::assertSame('[{"at":"01/10/2023 12:02","on":"2023/10/01"}]', $stream->content());
    }

    public function test_a_flag_the_native_writer_does_not_render_is_written_through_php(): void
    {
        $stream = new StringDestinationStream(path('memory://out.json'));
        $writer = (new JsonLoader(path('memory://out.json'), memory_filesystem(), JsonFraming::Lines))
            ->withFlags(JSON_THROW_ON_ERROR | JSON_HEX_TAG)
            ->writer($stream, schema(str_schema('tag')), new PhpBackend());

        $writer->write(array_to_rows([['tag' => '<a>']], schema(str_schema('tag'))));
        $writer->close();

        static::assertSame("{\"tag\":\"\\u003Ca\\u003E\"}\n", $stream->content());
    }

    public function test_a_flow_php_of_another_abi_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('does not match flow-php/etl');

        (new JsonLoader(
            path('memory://out.json'),
            memory_filesystem(),
            JsonFraming::Array,
            new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'),
        ))->writer(new StringDestinationStream(path('memory://out.json')), schema(int_schema('id')), new PhpBackend());
    }

    public function test_in_task_keeps_the_lane_override(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('does not match flow-php/etl');

        (new JsonLoader(
            path('memory://out.json'),
            memory_filesystem(),
            JsonFraming::Array,
            new FlowPhpExtension(true, FlowPhpExtension::ABI + 1, '0.46.0'),
        ))
            ->inTask(new Part(0, 1, '1a2b3c4d5e6f7a8b', false))
            ->writer(
                new StringDestinationStream(path('memory://other.json')),
                schema(int_schema('id')),
                new PhpBackend(),
            );
    }
}
