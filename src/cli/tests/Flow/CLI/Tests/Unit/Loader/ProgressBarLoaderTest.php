<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Unit\Loader;

use Flow\CLI\Loader\ProgressBarLoader;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Output\BufferedOutput;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class ProgressBarLoaderTest extends TestCase
{
    public function test_it_advances_the_bar_by_the_batch_size(): void
    {
        $progressBar = new ProgressBar(new BufferedOutput());

        (new ProgressBarLoader($progressBar))->load(
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3])),
            flow_context(),
        );

        static::assertSame(3, $progressBar->getProgress());
    }

    public function test_every_batch_adds_to_the_progress_so_far(): void
    {
        $progressBar = new ProgressBar(new BufferedOutput());
        $loader = new ProgressBarLoader($progressBar);
        $context = flow_context();

        $loader->load(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])), $context);
        $loader->load(rows(schema(int_schema('id')), row(['id' => 3])), $context);

        static::assertSame(3, $progressBar->getProgress());
    }

    public function test_an_empty_batch_leaves_the_bar_where_it_was(): void
    {
        $progressBar = new ProgressBar(new BufferedOutput());

        (new ProgressBarLoader($progressBar))->load(rows(schema(int_schema('id'))), flow_context());

        static::assertSame(0, $progressBar->getProgress());
    }
}
