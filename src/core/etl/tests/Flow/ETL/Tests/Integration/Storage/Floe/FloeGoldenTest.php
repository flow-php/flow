<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Storage\Floe;

use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\Context\FloeGoldenContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Rows;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_merge;
use function iterator_to_array;
use function ord;

/**
 * Pinned bytes: an unintended format change fails here, an intended one rebaselines in the same commit.
 */
final class FloeGoldenTest extends FlowIntegrationTestCase
{
    public static function golden_fixture_names(): Generator
    {
        foreach (FloeGoldenContext::batches() as $name => $_) {
            yield $name => [$name];
        }
    }

    public static function golden_fixtures(): Generator
    {
        foreach (FloeGoldenContext::batches() as $name => $batches) {
            yield $name => [$name, $batches];
        }
    }

    /**
     * @param list<Rows> $batches
     */
    #[DataProvider('golden_fixtures')]
    public function test_golden_bytes_are_unchanged(string $name, array $batches): void
    {
        $written = $this->cacheDir->suffix('golden-' . $name . '.floe');

        FloeFilesContext::writeAll(
            FloeFilesContext::files($this->fs())->create($written, FloeGoldenContext::schema($batches)),
            $batches,
        );

        static::assertSame(
            $this->fs()->readFrom(FloeGoldenContext::path($name))->content(),
            $this->fs()->readFrom($written)->content(),
        );
    }

    /**
     * @param list<Rows> $batches
     */
    #[DataProvider('golden_fixtures')]
    public function test_golden_files_read_back_to_the_rows_that_wrote_them(string $name, array $batches): void
    {
        $file = FloeFilesContext::files($this->fs())->read(FloeGoldenContext::path($name));
        $read = iterator_to_array($file->rows(), false);
        $file->close();

        static::assertEquals(
            array_merge(...array_map(static fn(Rows $batch) => $batch->toArray(), $batches)),
            array_merge(...array_map(static fn(Rows $batch) => $batch->toArray(), $read)),
        );
    }

    #[DataProvider('golden_fixture_names')]
    public function test_golden_header_is_the_current_format_version(string $name): void
    {
        static::assertSame(0x02, ord($this->fs()->readFrom(FloeGoldenContext::path($name))->content()[4]));
    }

    #[DataProvider('golden_fixture_names')]
    public function test_golden_fixtures_hold_batch_frames(string $name): void
    {
        static::assertSame(
            $name === 'empty' ? 0x06 : 0x05,
            ord($this->fs()->readFrom(FloeGoldenContext::path($name))->content()[6]),
        );
    }
}
