<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Filesystem;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use Throwable;

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\Adapter\JSON\to_json_lines;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\Adapter\XML\to_xml;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\overwrite;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\to_floe;
use function iterator_to_array;
use function substr_count;

/**
 * The write-side properties that must survive the stream registry moving onto the sink: a header written
 * exactly once, a close tag written once per file, and Overwrite's tmp->final rename with its stale sweep.
 */
final class SinkWritePropertiesTest extends FlowIntegrationTestCase
{
    /**
     * A format writer emits its footer when it closes, so closing a dead run's writer turns a half-written file into
     * a complete, readable one. Whatever the save mode, that file must not be left at the destination.
     */
    #[TestWith(['csv'])]
    #[TestWith(['json'])]
    #[TestWith(['jsonl'])]
    #[TestWith(['txt'])]
    #[TestWith(['xml'])]
    #[TestWith(['xlsx'])]
    #[TestWith(['parquet'])]
    #[TestWith(['floe'])]
    public function test_a_failed_run_leaves_no_file_at_the_destination(string $format): void
    {
        $destination = $this->cacheDir->suffix('aborted.' . $format);
        $sink = match ($format) {
            'csv' => to_csv($destination),
            'json' => to_json($destination),
            'jsonl' => to_json_lines($destination),
            'txt' => to_text($destination),
            'xml' => to_xml($destination),
            'xlsx' => to_excel($destination),
            'parquet' => to_parquet($destination),
            default => to_floe($destination),
        };

        try {
            df()
                ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
                ->batchSize(1)
                ->with(new ThrowWhenRowMatches('id', 3, new RuntimeException('aborted')))
                ->write($sink)
                ->run();
            static::fail('The run was expected to fail');
        } catch (Throwable $e) {
            static::assertSame('aborted', $e->getMessage());
        }

        static::assertNull($this->fs()->status($destination));
    }

    public function test_csv_header_is_written_exactly_once_across_batches(): void
    {
        $destination = $this->cacheDir->suffix('header-once.csv');

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]]))
            ->batchSize(1)
            ->write(to_csv($destination))
            ->run();

        static::assertSame(1, substr_count($this->fs()->readFrom($destination)->content(), 'id'));
    }

    public function test_json_close_tag_is_written_once_per_partition_file(): void
    {
        $destination = $this->cacheDir->suffix('close-tag.json');

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->batchSize(1)
            ->write(to_json($destination))
            ->run();

        $content = $this->fs()->readFrom($destination)->content();

        static::assertSame(1, substr_count($content, '['));
        static::assertSame(1, substr_count($content, ']'));
    }

    public function test_overwrite_promotes_the_tmp_file(): void
    {
        $destination = $this->cacheDir->suffix('overwrite.csv');

        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_csv($destination))
            ->run();
        df()
            ->read(from_array([['id' => 2]]))
            ->write(to_csv($destination)->saveMode(overwrite()))
            ->run();

        static::assertSame("id\n2\n", $this->fs()->readFrom($destination)->content());
        static::assertSame(
            [],
            iterator_to_array($this->fs()->list(path($this->cacheDir->path() . '/._flow_php_tmp.*')), false),
        );
    }

    public function test_save_mode_is_per_sink(): void
    {
        $overwritten = $this->cacheDir->suffix('per-sink-overwrite.csv');
        $strict = $this->cacheDir->suffix('per-sink-strict.csv');

        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_csv($overwritten))
            ->run();
        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_csv($strict))
            ->run();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('already exists');

        // the first sink overwrites, the second must still refuse - the mode does not leak between them
        df()
            ->read(from_array([['id' => 2]]))
            ->write(to_csv($overwritten)->saveMode(overwrite()))
            ->write(to_csv($strict))
            ->run();
    }

    public function test_two_distinct_sinks_on_one_path_throw(): void
    {
        $destination = $this->cacheDir->suffix('two-sinks.csv');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('already exists');

        df()
            ->read(from_array([['id' => 1]]))
            ->write(to_csv($destination))
            ->write(to_csv($destination))
            ->run();
    }

    public function test_xml_close_tag_is_written_once(): void
    {
        $destination = $this->cacheDir->suffix('close-tag.xml');

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->batchSize(1)
            ->write(to_xml($destination))
            ->run();

        static::assertSame(1, substr_count($this->fs()->readFrom($destination)->content(), '</rows>'));
    }
}
