<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Loader;

use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_branch;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;

final class PartitionedWriteTest extends FlowIntegrationTestCase
{
    public function test_one_batch_with_two_combinations_writes_two_directories(): void
    {
        $output = $this->cacheDir->suffix('/b39');

        df()
            ->read(from_array([
                ['region' => 'eu', 'text' => 'a'],
                ['region' => 'us', 'text' => 'b'],
                ['region' => 'eu', 'text' => 'c'],
            ]))
            ->write(
                to_floe($output->suffix('/out.floe')->path())
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('region')),
            )
            ->run();

        static::assertFileExists($output->suffix('/region=eu/out.floe')->path());
        static::assertFileExists($output->suffix('/region=us/out.floe')->path());
    }

    public function test_the_partition_column_is_not_written_into_the_body(): void
    {
        $output = $this->cacheDir->suffix('/stripped');

        df()
            ->read(from_array([['region' => 'eu', 'text' => 'a']]))
            ->write(
                to_floe($output->suffix('/out.floe')->path())
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('region')),
            )
            ->run();

        static::assertSame(
            [
                'text',
            ],
            FloeFilesContext::columns(native_local_filesystem(), $output->suffix('/region=eu/out.floe')),
        );
    }

    public function test_the_partition_column_comes_back_from_the_path(): void
    {
        $output = $this->cacheDir->suffix('/round-trip');

        df()
            ->read(from_array([['region' => 'eu', 'text' => 'a'], ['region' => 'us', 'text' => 'b']]))
            ->write(
                to_floe($output->suffix('/out.floe')->path())
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('region')),
            )
            ->run();

        $rows = df()
            ->read(from_floe($output->suffix('/*/*.floe')->path()))
            ->sortBy([ref('text')])
            ->fetch();

        static::assertSame([['text' => 'a', 'region' => 'eu'], ['text' => 'b', 'region' => 'us']], $rows->toArray());
    }

    /**
     * Impossible before phase 4: partitioning lived on the batch, so two branches of one stream could
     * not disagree about it.
     */
    public function test_two_branches_partition_independently(): void
    {
        $output = $this->cacheDir->suffix('/branches');

        df()
            ->read(from_array([
                ['region' => 'eu', 'year' => '2024', 'text' => 'a'],
                ['region' => 'us', 'year' => '2025', 'text' => 'b'],
            ]))
            ->write(to_branch(
                ref('text')->isNotNull(),
                to_floe($output->suffix('/by-region/out.floe')->path())
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('region')),
            ))
            ->write(to_branch(
                ref('text')->isNotNull(),
                to_floe($output->suffix('/by-year/out.floe')->path())
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('year')),
            ))
            ->run();

        static::assertFileExists($output->suffix('/by-region/region=eu/out.floe')->path());
        static::assertFileExists($output->suffix('/by-region/region=us/out.floe')->path());
        static::assertFileExists($output->suffix('/by-year/year=2024/out.floe')->path());
        static::assertFileExists($output->suffix('/by-year/year=2025/out.floe')->path());
    }

    public function test_a_null_partition_value_writes_the_hive_default_directory(): void
    {
        $output = $this->cacheDir->suffix('/nulls');

        df()
            ->read(from_array([['region' => 'eu', 'text' => 'a'], ['region' => null, 'text' => 'b']]))
            ->write(
                to_floe($output->suffix('/out.floe')->path())
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('region')),
            )
            ->run();

        static::assertFileExists($output->suffix('/region=eu/out.floe')->path());
        static::assertFileExists($output->suffix('/region=__HIVE_DEFAULT_PARTITION__/out.floe')->path());
    }

    public function test_a_partition_value_carrying_a_reserved_character_is_encoded(): void
    {
        $output = $this->cacheDir->suffix('/encoded');

        df()
            ->read(from_array([['region' => 'eu/west', 'text' => 'a']]))
            ->write(
                to_floe($output->suffix('/out.floe')->path())
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('region')),
            )
            ->run();

        static::assertFileExists($output->suffix('/region=eu%2Fwest/out.floe')->path());
        static::assertSame(
            [['text' => 'a', 'region' => 'eu/west']],
            df()
                ->read(from_floe($output->suffix('/*/*.floe')->path()))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_write_columns_keeps_the_column_in_the_body_as_well(): void
    {
        $output = $this->cacheDir->suffix('/write-columns');

        df()
            ->read(from_array([['region' => 'eu', 'text' => 'a']]))
            ->write(
                to_floe($output->suffix('/out.floe')->path())
                    ->saveMode(overwrite())
                    ->partitionBy(partition_by('region')->writeColumns()),
            )
            ->run();

        static::assertSame(
            ['region', 'text'],
            FloeFilesContext::columns(native_local_filesystem(), $output->suffix('/region=eu/out.floe')),
        );
    }
}
