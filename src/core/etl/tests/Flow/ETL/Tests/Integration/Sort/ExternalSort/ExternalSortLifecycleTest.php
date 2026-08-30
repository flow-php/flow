<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Sort\ExternalSort;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class ExternalSortLifecycleTest extends FlowIntegrationTestCase
{
    public function test_a_clean_run_leaves_no_spilled_runs(): void
    {
        $output = [];
        df(config_builder()->sort(external_sort()->runSize(1)->bucketsCount(2))->build())
            ->read(from_array($this->descendingIds()))
            ->sortBy([ref('id')])
            ->write(to_array($output))
            ->run();

        static::assertCount(20, $output);
        static::assertSame([], iterator_to_array($this->fs->list(path($this->cacheDir->path() . '/**/*.floe')), false));
    }

    public function test_upstream_failure_leaves_no_spilled_runs(): void
    {
        $output = [];

        try {
            df(config_builder()->sort(external_sort()->runSize(1)->bucketsCount(2))->build())
                ->read(from_array($this->descendingIds()))
                ->map(schema(int_schema('id')), static function (Row $row): Row {
                    if ($row->get('id') === 5) {
                        throw new RuntimeException('upstream failed mid-bucketing');
                    }

                    return $row;
                })
                ->sortBy([ref('id')])
                ->write(to_array($output))
                ->run();

            static::fail('The upstream failure must escape.');
        } catch (RuntimeException $escaped) {
            static::assertSame('upstream failed mid-bucketing', $escaped->getMessage());
        }

        static::assertSame([], iterator_to_array($this->fs->list(path($this->cacheDir->path() . '/**/*.floe')), false));
    }

    /**
     * @return list<array{id: int}>
     */
    public function descendingIds(): array
    {
        $rows = [];

        for ($id = 20; $id > 0; $id--) {
            $rows[] = ['id' => $id];
        }

        return $rows;
    }
}
