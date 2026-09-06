<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Extractor\SpilledRows;
use Flow\ETL\Extractor\SpillState;
use Flow\ETL\Tests\Double\RecordingFilesystem;
use Flow\ETL\Tests\Double\ThrowingRmFilesystem;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Exception\RuntimeException;
use Generator;

use function array_filter;
use function count;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class SpilledRowsTest extends FlowTestCase
{
    public function test_a_fresh_sampler_writes_no_file(): void
    {
        $filesystem = memory_filesystem();
        $spill = new SpilledRows([['id' => 1]], $filesystem, path('memory://tmp'));

        static::assertSame(SpillState::Fresh, $spill->state());
        static::assertNull($filesystem->status($spill->path()));
    }

    public function test_a_replay_yields_the_rows_that_were_spilled(): void
    {
        $rows = [
            [
                'at' => new DateTimeImmutable('2024-01-01 09:00:00.123456'),
                'backed' => BackedStringEnum::one,
                'pure' => BasicEnum::two,
            ],
            [
                'nested' => ['a' => [1, 2, null], 'b' => null],
                'text' => "line one\nline two\r\nend",
                'binary' => "\x00\x01\xff binary",
            ],
        ];
        $spill = new SpilledRows($rows, memory_filesystem(), path('memory://tmp'));

        iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);

        static::assertEquals($rows, iterator_to_array($spill->rows(), false));
    }

    public function test_a_spill_can_be_replayed_more_than_once(): void
    {
        $spill = new SpilledRows([['id' => 1], ['id' => 2]], memory_filesystem(), path('memory://tmp'));

        iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);

        // The source was read once; the FILE it produced is re-readable, so repeated terminal
        // operations on the wrapping DataFrame behave the way they do for an array source.
        static::assertSame([['id' => 1], ['id' => 2]], iterator_to_array($spill->rows(), false));
        static::assertSame([['id' => 1], ['id' => 2]], iterator_to_array($spill->rows(), false));
        static::assertSame(SpillState::Spilled, $spill->state());
    }

    public function test_a_second_samples_is_refused(): void
    {
        $spill = new SpilledRows([['id' => 1]], memory_filesystem(), path('memory://tmp'));

        iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('The dataset was already consumed');

        $spill->samples(-1);
    }

    public function test_a_source_that_throws_mid_spill_leaves_the_sampler_abandoned(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());

        /** @var callable(): Generator<int, array<string, mixed>> $source */
        $source = static function (): Generator {
            yield ['id' => 1];

            throw new RuntimeException('the source broke');
        };
        $spill = new SpilledRows($source(), $filesystem, path('memory://tmp'));

        try {
            iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);
        } catch (RuntimeException) {
        }

        static::assertSame(SpillState::Abandoned, $spill->state());
        static::assertContains('close', $filesystem->calls);

        $path = $spill->path();

        static::assertNotNull($filesystem->status($path));

        unset($spill);

        static::assertNull($filesystem->status($path));
    }

    public function test_destruct_removes_a_spill_that_was_never_replayed(): void
    {
        $filesystem = memory_filesystem();
        $spill = new SpilledRows([['id' => 1]], $filesystem, path('memory://tmp'));
        $path = $spill->path();

        iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);

        static::assertNotNull($filesystem->status($path));

        unset($spill);

        static::assertNull($filesystem->status($path));
    }

    public function test_destruct_swallows_a_filesystem_that_throws(): void
    {
        $this->expectNotToPerformAssertions();

        $spill = new SpilledRows([['id' => 1]], new ThrowingRmFilesystem(memory_filesystem()), path('memory://tmp'));

        iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);

        unset($spill);
    }

    public function test_a_buffered_spill_writes_fewer_times_and_still_holds_every_row(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());

        /** @var callable(): Generator<int, array<string, mixed>> $source */
        $source = static function (): Generator {
            for ($id = 0; $id < 200; $id++) {
                yield ['id' => $id];
            }
        };
        $spill = new SpilledRows($source(), $filesystem, path('memory://tmp'));

        iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);

        static::assertLessThan(
            200,
            count(array_filter($filesystem->calls, static fn(string $c): bool => $c === 'append')),
        );
        static::assertCount(200, iterator_to_array($spill->rows(), false));
    }

    public function test_each_row_is_written_before_the_next_is_pulled_when_unbuffered(): void
    {
        $filesystem = new RecordingFilesystem(memory_filesystem());

        /** @var callable(): Generator<int, array<string, mixed>> $source */
        $source = static function () use ($filesystem): Generator {
            foreach ([['id' => 1], ['id' => 2], ['id' => 3]] as $row) {
                $filesystem->record('pull');

                yield $row;
            }
        };
        $spill = new SpilledRows($source(), $filesystem, path('memory://tmp'), bufferSize: 1);

        iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);

        static::assertSame(
            ['writeTo', 'pull', 'append', 'pull', 'append', 'pull', 'append', 'close'],
            $filesystem->calls,
        );
    }

    public function test_rows_on_a_fresh_sampler_streams_the_source_once_and_writes_nothing(): void
    {
        $filesystem = memory_filesystem();
        $spill = new SpilledRows([['id' => 1], ['id' => 2]], $filesystem, path('memory://tmp'));

        static::assertSame([['id' => 1], ['id' => 2]], iterator_to_array($spill->rows(), false));
        static::assertNull($filesystem->status($spill->path()));
    }

    public function test_rows_refuses_an_abandoned_sampler(): void
    {
        /** @var callable(): Generator<int, array<string, mixed>> $source */
        $source = static function (): Generator {
            yield ['id' => 1];

            throw new RuntimeException('the source broke');
        };
        $spill = new SpilledRows($source(), memory_filesystem(), path('memory://tmp'));

        try {
            iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);
        } catch (RuntimeException) {
        }

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('The dataset was already consumed');

        $spill->rows();
    }

    public function test_samples_spills_every_row_and_reaches_the_spilled_state(): void
    {
        $filesystem = memory_filesystem();
        $spill = new SpilledRows([['id' => 1], ['id' => 2], ['id' => 3]], $filesystem, path('memory://tmp'));

        iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);

        static::assertSame(SpillState::Spilled, $spill->state());
        static::assertCount(3, iterator_to_array($filesystem->readFrom($spill->path())->readLines("\n"), false));
    }

    public function test_samples_throws_before_the_returned_generator_is_advanced(): void
    {
        $spill = new SpilledRows([['id' => 1]], memory_filesystem(), path('memory://tmp'));

        $spill->samples(-1);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('The dataset was already consumed');

        $spill->samples(-1);
    }

    public function test_the_row_budget_is_ignored_and_the_whole_source_is_still_spilled(): void
    {
        $filesystem = memory_filesystem();
        $spill = new SpilledRows([['id' => 1], ['id' => 2], ['id' => 3]], $filesystem, path('memory://tmp'));

        iterator_to_array(iterator_to_array($spill->samples(1), false)[0], false);

        static::assertCount(3, iterator_to_array($filesystem->readFrom($spill->path())->readLines("\n"), false));
    }

    public function test_the_spill_file_outlives_the_replay_and_is_removed_with_the_sampler(): void
    {
        $filesystem = memory_filesystem();
        $spill = new SpilledRows([['id' => 1]], $filesystem, path('memory://tmp'));
        $path = $spill->path();

        iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);
        iterator_to_array($spill->rows(), false);

        static::assertSame(SpillState::Spilled, $spill->state());
        static::assertNotNull($filesystem->status($path));

        unset($spill);

        static::assertNull($filesystem->status($path));
    }

    public function test_the_spill_file_is_removed_after_an_abandoned_replay(): void
    {
        $filesystem = memory_filesystem();
        $spill = new SpilledRows([['id' => 1], ['id' => 2]], $filesystem, path('memory://tmp'));
        $path = $spill->path();

        iterator_to_array(iterator_to_array($spill->samples(-1), false)[0], false);

        foreach ($spill->rows() as $_row) {
            break;
        }

        unset($spill);

        static::assertNull($filesystem->status($path));
    }
}
