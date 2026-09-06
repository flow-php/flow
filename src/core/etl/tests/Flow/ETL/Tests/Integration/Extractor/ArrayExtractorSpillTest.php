<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Extractor;

use Flow\ETL\Tests\FlowIntegrationTestCase;
use Generator;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\execution_context;
use function Flow\ETL\DSL\from_array;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class ArrayExtractorSpillTest extends FlowIntegrationTestCase
{
    public function test_the_spill_holds_one_line_per_row(): void
    {
        $spillRoot = path(__DIR__ . '/var/array_spill_frames');
        $this->fs()->rm($spillRoot);

        $extractor = from_array(
            (
                /** @return Generator<int, array<string, mixed>> */
                static function (): Generator {
                    yield ['id' => 1];
                    yield ['id' => 2];
                    yield ['id' => 3];
                }
            )(),
            filesystem: $this->fs(),
            spillRoot: $spillRoot,
        );

        $extractor->schema();

        $spilled = iterator_to_array($this->fs()->list($spillRoot->suffix('/flow-php-source/*.b64')), false);

        static::assertCount(1, $spilled);
        static::assertCount(3, iterator_to_array($this->fs()->readFrom($spilled[0]->path)->readLines("\n"), false));

        $this->fs()->rm($spillRoot);
    }

    public function test_the_spill_outlives_the_replay_and_is_removed_with_the_extractor(): void
    {
        $spillRoot = path(__DIR__ . '/var/array_spill_replayed');
        $this->fs()->rm($spillRoot);

        $extractor = from_array(
            (
                /** @return Generator<int, array<string, mixed>> */
                static function (): Generator {
                    yield ['id' => 1];
                    yield ['id' => 2];
                }
            )(),
            filesystem: $this->fs(),
            spillRoot: $spillRoot,
        );

        static::assertCount(2, iterator_to_array($extractor->extract(execution_context(config()))));
        static::assertCount(1, iterator_to_array(
            $this->fs()->list($spillRoot->suffix('/flow-php-source/*.b64')),
            false,
        ));

        unset($extractor);

        static::assertSame(
            [],
            iterator_to_array($this->fs()->list($spillRoot->suffix('/flow-php-source/*.b64')), false),
        );

        $this->fs()->rm($spillRoot);
    }

    public function test_the_spill_is_removed_when_extract_never_runs(): void
    {
        $spillRoot = path(__DIR__ . '/var/array_spill_described_only');
        $this->fs()->rm($spillRoot);

        $extractor = from_array(
            (
                /** @return Generator<int, array<string, mixed>> */
                static function (): Generator {
                    yield ['id' => 1];
                }
            )(),
            filesystem: $this->fs(),
            spillRoot: $spillRoot,
        );
        $extractor->schema();

        static::assertCount(1, iterator_to_array(
            $this->fs()->list($spillRoot->suffix('/flow-php-source/*.b64')),
            false,
        ));

        unset($extractor);

        static::assertSame(
            [],
            iterator_to_array($this->fs()->list($spillRoot->suffix('/flow-php-source/*.b64')), false),
        );

        $this->fs()->rm($spillRoot);
    }

    public function test_the_spill_is_removed_when_the_replay_stops_early(): void
    {
        $spillRoot = path(__DIR__ . '/var/array_spill_limited');
        $this->fs()->rm($spillRoot);

        $rows = data_frame()
            ->read(from_array(
                (
                    /** @return Generator<int, array<string, mixed>> */
                    static function (): Generator {
                        for ($i = 0; $i < 50; $i++) {
                            yield ['id' => $i];
                        }
                    }
                )(),
                filesystem: $this->fs(),
                spillRoot: $spillRoot,
            ))
            ->limit(1)
            ->fetch();

        static::assertCount(1, $rows);
        static::assertSame(
            [],
            iterator_to_array($this->fs()->list($spillRoot->suffix('/flow-php-source/*.b64')), false),
        );

        $this->fs()->rm($spillRoot);
    }
}
