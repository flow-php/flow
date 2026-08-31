<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Tests\Context\FloeEngineContext;
use Flow\Floe\Tests\Context\FloeGoldenContext;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function ord;

/**
 * The committed files are the only evidence that a format change was intended. Two live writers
 * compared against each other re-baseline themselves on every change, so the bytes are pinned here
 * instead: a change that was not meant to touch the format shows up as a failure, and one that was
 * shows up as a deliberate rebaseline in the same commit.
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

        FloeEngineContext::writeAll(
            FloeEngineContext::phpWriter($this->fs(), FloeGoldenContext::schema($batches)),
            $written,
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
        $expected = [];

        foreach ($batches as $batch) {
            foreach ($batch as $row) {
                $expected[] = $row;
            }
        }

        $read = [];
        $reader = FloeEngineContext::phpReader($this->fs())->read(FloeGoldenContext::path($name));

        // conform: false keeps an absent column absent instead of padding it with null, which is the
        // distinction the bytes actually encode and the one a format golden has to pin
        foreach ($reader->rows(conform: false) as $batch) {
            foreach ($batch as $row) {
                $read[] = $row;
            }
        }

        $reader->close();

        static::assertEquals($expected, $read);
    }

    #[DataProvider('golden_fixture_names')]
    public function test_golden_header_is_the_current_format_version(string $name): void
    {
        static::assertSame(0x02, ord($this->fs()->readFrom(FloeGoldenContext::path($name))->content()[4]));
    }
}
