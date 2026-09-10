<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Datasets;

use Flow\Benchmarks\Datasets\FixtureFormat;
use Flow\Benchmarks\Datasets\FixturePath;
use Flow\Benchmarks\Datasets\Paths;
use PHPUnit\Framework\TestCase;

use function file_exists;
use function file_put_contents;
use function is_dir;
use function mkdir;
use function str_contains;
use function unlink;

/**
 * FixturePath always resolves under Paths::datasets(). These tests therefore use a prefix no real
 * fixture uses, and prune() only ever globs its own prefix and row count.
 */
final class FixturePathTest extends TestCase
{
    public function test_path_carries_the_fingerprint_segment(): void
    {
        static::assertMatchesRegularExpression(
            '#/fixture_path_test_1\.[0-9a-f]{8}\.csv$#',
            (new FixturePath('fixture_path_test', 1, FixtureFormat::csv))->path(),
        );
    }

    public function test_prune_deletes_a_differently_fingerprinted_sibling(): void
    {
        $fixture = new FixturePath('fixture_path_test', 2, FixtureFormat::csv);
        $stale = Paths::datasets() . '/fixture_path_test_2.00000000.csv';

        if (!is_dir(Paths::datasets())) {
            mkdir(Paths::datasets(), 0777, true);
        }

        file_put_contents($stale, 'stale');

        try {
            $fixture->prune();

            static::assertFalse(file_exists($stale));
        } finally {
            if (file_exists($stale)) {
                unlink($stale);
            }
        }
    }

    public function test_prune_deletes_the_pre_fingerprint_name(): void
    {
        $fixture = new FixturePath('fixture_path_test', 3, FixtureFormat::csv);
        $legacy = Paths::datasets() . '/fixture_path_test_3.csv';

        if (!is_dir(Paths::datasets())) {
            mkdir(Paths::datasets(), 0777, true);
        }

        file_put_contents($legacy, 'legacy');

        try {
            $fixture->prune();

            static::assertFalse(file_exists($legacy));
        } finally {
            if (file_exists($legacy)) {
                unlink($legacy);
            }
        }
    }

    public function test_prune_keeps_the_current_path_and_every_other_prefix(): void
    {
        $fixture = new FixturePath('fixture_path_test', 4, FixtureFormat::csv);
        $foreign = Paths::datasets() . '/orders_4.00000000.csv';

        if (!is_dir(Paths::datasets())) {
            mkdir(Paths::datasets(), 0777, true);
        }

        file_put_contents($fixture->path(), 'current');
        file_put_contents($foreign, 'foreign');

        try {
            $fixture->prune();

            $currentSurvived = file_exists($fixture->path());
            $foreignSurvived = file_exists($foreign);
        } finally {
            foreach ([$fixture->path(), $foreign] as $file) {
                if (file_exists($file)) {
                    unlink($file);
                }
            }
        }

        static::assertTrue($currentSurvived);
        static::assertTrue($foreignSurvived);
        static::assertTrue(str_contains($fixture->path(), '/benchmarks/datasets/'));
    }
}
