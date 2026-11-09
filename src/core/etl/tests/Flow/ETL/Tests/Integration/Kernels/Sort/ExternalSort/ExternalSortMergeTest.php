<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Kernels\Sort\ExternalSort;

use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\IdRows;
use Flow\ETL\Tests\Double\RecordingRowsStorage;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Tests\Mother\ExternalSortMother;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Memory\Unit;
use Flow\Filesystem\Path\Filter\KeepAll;
use Generator;

use function array_column;
use function array_filter;
use function chr;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function memory_get_peak_usage;
use function memory_get_usage;
use function memory_reset_peak_usage;
use function range;
use function str_repeat;
use function str_starts_with;

final class ExternalSortMergeTest extends FlowIntegrationTestCase
{
    public function test_a_merge_under_an_exhausted_budget_opens_few_runs_at_once(): void
    {
        $backend = new PhpBackend();
        // 250 runs of 40 rows, ~80 KB each: opening all 100 the fan-in allows holds ~13 MB, two at a time ~3 MB
        $input = (static function (): Generator {
            for ($run = 0; $run < 250; $run++) {
                $rows = [];

                for ($row = 0; $row < 40; $row++) {
                    $rows[] = ['id' => ($row * 250) + $run, 'payload' => str_repeat(chr(65 + ($run % 26)), 2_000)];
                }

                yield array_to_rows($rows, schema(int_schema('id'), str_schema('payload')));
            }
        })();

        memory_reset_peak_usage();
        $before = memory_get_usage();

        $sort = ExternalSortMother::with(
            [ref('id')->asc()],
            new FilesystemRowsStorage($this->fs, path($this->cacheDir->path() . '/sort-runs'), $backend, 40),
            100,
            40,
        );
        $ids = IdRows::ids($sort->sort($input, $backend, new MemoryBudget($backend, Unit::fromBytes(1), null)));

        static::assertSame(range(0, 9_999), $ids);
        static::assertLessThan(Unit::fromMb(8)->inBytes(), memory_get_peak_usage() - $before);
    }

    public function test_external_sort_over_three_merge_generations_loses_no_rows(): void
    {
        $input = [];

        for ($id = 16; $id > 0; $id--) {
            $input[] = ['id' => $id];
        }

        $output = [];
        df(config_builder()->sort(external_sort()->memoryLimit(Unit::fromBytes(1))->bucketsCount(2))->build())
            ->read(from_array($input))
            ->batchSize(2)
            ->sortBy([ref('id')])
            ->write(to_array($output))
            ->run();

        static::assertSame(range(1, 16), array_column($output, 'id'));
    }

    public function test_spilled_runs_are_read_back_through_the_configured_backend(): void
    {
        $backend = new SpyBackend();
        $output = [];

        df(
            config_builder()
                ->backend($backend)
                ->sort(external_sort()->memoryLimit(Unit::fromBytes(1)))
                ->build(),
        )
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 2]]))
            ->batchSize(1)
            ->sortBy(ref('id'))
            ->write(to_array($output))
            ->run();

        static::assertSame([1, 2, 3], array_column($output, 'id'));
        static::assertGreaterThan(0, $backend->decodeFrames());
    }

    public function test_memory_storage_spills_nothing_to_disk(): void
    {
        $input = [];

        for ($id = 100; $id > 0; $id--) {
            $input[] = ['id' => $id];
        }

        $output = [];
        df(
            config_builder()
                ->sort(
                    external_sort()->storage(new MemoryRowsStorage())->memoryLimit(Unit::fromBytes(1))->bucketsCount(2),
                )
                ->build(),
        )
            ->read(from_array($input))
            ->batchSize(1)
            ->sortBy([ref('id')])
            ->write(to_array($output))
            ->run();

        static::assertSame(range(1, 100), array_column($output, 'id'));
        // .floe files are removed by the run's own clear(), so the pin is that nothing was ever created:
        // a filesystem-backed spill or merge side leaves /flow-php-sort/flow-php-rows/ behind
        static::assertSame(
            [],
            iterator_to_array($this->fs->list(path($this->cacheDir->path() . '/**/*'), new KeepAll()), false),
        );
    }

    public function test_merge_storage_receives_only_merged_runs(): void
    {
        $spill = new RecordingRowsStorage(new MemoryRowsStorage());
        $merge = new RecordingRowsStorage(new MemoryRowsStorage());

        $input = [];

        for ($id = 100; $id > 0; $id--) {
            $input[] = ['id' => $id];
        }

        $output = [];
        df(
            config_builder()
                ->sort(
                    external_sort()
                        ->storage($spill)
                        ->mergeStorage($merge)
                        ->memoryLimit(Unit::fromBytes(1))
                        ->bucketsCount(2),
                )
                ->build(),
        )
            ->read(from_array($input))
            ->batchSize(1)
            ->sortBy([ref('id')])
            ->write(to_array($output))
            ->run();

        static::assertSame(range(1, 100), array_column($output, 'id'));
        static::assertNotSame([], $merge->appended, 'the run count must force at least one merged generation');
        static::assertSame([], array_filter($spill->read, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
        static::assertCount(0, array_filter(
            $merge->read,
            static fn(string $id): bool => !str_starts_with($id, 'sort-merge-'),
        ));
    }
}
