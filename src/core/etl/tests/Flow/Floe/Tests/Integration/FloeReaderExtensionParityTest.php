<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\Format;
use Flow\Floe\RowEncoder;
use Flow\Floe\Tests\Context\FloeFileContext;
use Flow\Floe\Tests\Mother\RowsMother;
use Override;
use PHPUnit\Framework\Attributes\DataProvider;

use function extension_loaded;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function iterator_to_array;
use function pack;

/**
 * With the flow_php extension loaded, FloeReader hydrates ROW frame bodies
 * through the extension - these tests pin the extension path to the pure-PHP
 * hydrator, which stays the canonical behavior reference.
 */
final class FloeReaderExtensionParityTest extends FlowIntegrationTestCase
{
    public static function rows_datasets(): array
    {
        return [
            'all entry types' => [RowsMother::withAllEntryTypes()],
            'heterogeneous' => [RowsMother::heterogeneous()],
            'partitioned' => [RowsMother::partitioned()],
            'empty' => [rows()],
        ];
    }

    #[Override]
    protected function setUp(): void
    {
        parent::setUp();

        if (!extension_loaded('flow_php')) {
            self::markTestSkipped('flow_php extension is not loaded.');
        }
    }

    #[DataProvider('rows_datasets')]
    public function test_extension_and_pure_php_hydrate_identical_rows(Rows $rows): void
    {
        $path = $this->cacheDir->suffix('parity.floe');

        $writer = new FloeWriter($this->fs());
        $writer->create($path);
        $writer->write($rows);
        $writer->close();

        static::assertEquals(
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: false))
                    ->read($path)
                    ->rows(),
            ),
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: true))
                    ->read($path)
                    ->rows(),
            ),
        );
    }

    public function test_extension_and_pure_php_seek_offset_identically(): void
    {
        $path = $this->cacheDir->suffix('parity-offset.floe');

        $writer = new FloeWriter($this->fs());
        $writer->create($path);
        $writer->write(rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
            row(int_entry('id', 4), str_entry('email', null)),
            row(int_entry('id', 5), str_entry('email', 'x')),
            row(int_entry('id', 6)),
        ));
        $writer->close();

        static::assertEquals(
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: false))
                    ->read($path)
                    ->rows(1000, 2, 3),
            ),
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: true))
                    ->read($path)
                    ->rows(1000, 2, 3),
            ),
        );
    }

    #[DataProvider('rows_datasets')]
    public function test_extension_and_pure_php_recover_identical_rows(Rows $rows): void
    {
        $path = $this->cacheDir->suffix('parity-recover.floe');

        $writer = new FloeWriter($this->fs());
        $writer->create($path);
        $writer->write($rows);
        $writer->close();

        static::assertEquals(
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: false))
                    ->read($path)
                    ->recover(),
            ),
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: true))
                    ->read($path)
                    ->recover(),
            ),
        );
    }

    public function test_extension_and_pure_php_recover_a_torn_file_identically(): void
    {
        $path = $this->cacheDir->suffix('parity-recover-torn.floe');

        FloeFileContext::writeWithoutFooter(
            $this->fs(),
            $path,
            rows(row(int_entry('id', 1)), row(int_entry('id', 2), str_entry('email', 'x')), row(int_entry('id', 3))),
        );

        static::assertEquals(
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: false))
                    ->read($path)
                    ->recover(),
            ),
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: true))
                    ->read($path)
                    ->recover(),
            ),
        );
    }

    public function test_extension_and_pure_php_salvage_rows_before_a_corrupt_row_identically(): void
    {
        $path = $this->cacheDir->suffix('parity-recover-corrupt.floe');

        $plan = FloeWriter::growSectionPlan(null, row(int_entry('id', 1)));
        $rowEncoder = new RowEncoder();

        $stream = $this->fs()->writeTo($path);
        $stream->append(
            Format::header(0x00)
                . Format::frame(Format::FRAME_SCHEMA, $plan->schemaBody)
                . Format::frame(Format::FRAME_ROW, $rowEncoder->encode($plan, row(int_entry('id', 1))))
                . Format::frame(Format::FRAME_ROW, $rowEncoder->encode($plan, row(int_entry('id', 2))))
                . Format::frame(Format::FRAME_ROW, "\xEE"),
        );
        $stream->close();

        $pure = iterator_to_array(
            (new FloeReader($this->fs(), useExtension: false))
                ->read($path)
                ->recover(),
        );

        static::assertEquals(
            $pure,
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: true))
                    ->read($path)
                    ->recover(),
            ),
        );
        static::assertCount(1, $pure);
        static::assertCount(2, $pure[0]->all());
    }

    public function test_extension_and_pure_php_recover_rows_around_a_late_partitions_frame_identically(): void
    {
        $path = $this->cacheDir->suffix('parity-recover-late-partitions.floe');

        $plan = FloeWriter::growSectionPlan(null, row(int_entry('id', 1)));
        $rowEncoder = new RowEncoder();
        $partitionsBody = pack('V', 1) . pack('V', 1) . 'g' . pack('V', 1) . 'a';

        $stream = $this->fs()->writeTo($path);
        $stream->append(
            Format::header(0x00)
                . Format::frame(Format::FRAME_SCHEMA, $plan->schemaBody)
                . Format::frame(Format::FRAME_ROW, $rowEncoder->encode($plan, row(int_entry('id', 1))))
                . Format::frame(Format::FRAME_PARTITIONS, $partitionsBody)
                . Format::frame(Format::FRAME_ROW, $rowEncoder->encode($plan, row(int_entry('id', 2)))),
        );
        $stream->close();

        $pure = iterator_to_array(
            (new FloeReader($this->fs(), useExtension: false))
                ->read($path)
                ->recover(),
        );

        static::assertEquals(
            $pure,
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: true))
                    ->read($path)
                    ->recover(),
            ),
        );
        static::assertCount(1, $pure);
        static::assertCount(2, $pure[0]->all());
    }

    public function test_extension_and_pure_php_pad_evolved_sections_identically(): void
    {
        $path = $this->cacheDir->suffix('parity-evolved.floe');

        $writer = new FloeWriter($this->fs());
        $writer->create($path);
        $writer->write(rows(row(int_entry('id', 1))));
        $writer->close();

        $writer = new FloeWriter($this->fs());
        $writer->append($path);
        $writer->write(rows(row(int_entry('id', 2), str_entry('email', null))));
        $writer->close();

        static::assertEquals(
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: false))
                    ->read($path)
                    ->rows(),
            ),
            iterator_to_array(
                (new FloeReader($this->fs(), useExtension: true))
                    ->read($path)
                    ->rows(),
            ),
        );
    }
}
