<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Rows;
use Flow\ETL\Tests\Fixtures\CustomDateTime;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FloeWriter;
use Flow\Floe\Tests\Mother\RowsMother;
use Override;
use PHPUnit\Framework\Attributes\DataProvider;

use function extension_loaded;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

/**
 * With the flow_php extension loaded, FloeWriter encodes ROW frame bodies through
 * the extension's RowsEncoder. These tests pin the pure-PHP RowEncoder as the
 * canonical reference and assert the extension writes byte-identical files.
 */
final class FloeWriterExtensionParityTest extends FlowIntegrationTestCase
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

    public function test_extension_and_pure_php_reject_datetime_subclass_identically(): void
    {
        $rows = rows(row(datetime_entry('at', new CustomDateTime('2025-01-01 00:00:00 UTC'))));

        foreach ([false, true] as $useExtension) {
            $writer = new FloeWriter($this->fs(), useExtension: $useExtension);
            $writer->create($this->cacheDir->suffix('subclass-' . ($useExtension ? 'ext' : 'php') . '.floe'));

            try {
                $writer->write($rows);
                static::fail('Expected a FloeException for a custom datetime subclass');
            } catch (FloeException $e) {
                static::assertStringContainsString('DateTime and DateTimeImmutable', $e->getMessage());
            }
        }
    }

    #[DataProvider('rows_datasets')]
    public function test_extension_and_pure_php_write_byte_identical_files(Rows $rows): void
    {
        $purePath = $this->cacheDir->suffix('write-pure.floe');
        $extPath = $this->cacheDir->suffix('write-ext.floe');

        $pure = new FloeWriter($this->fs(), useExtension: false);
        $pure->create($purePath);
        $pure->write($rows);
        $pure->close();

        $ext = new FloeWriter($this->fs(), useExtension: true);
        $ext->create($extPath);
        $ext->write($rows);
        $ext->close();

        static::assertSame($this->fs()->readFrom($purePath)->content(), $this->fs()->readFrom($extPath)->content());
    }

    public function test_extension_and_pure_php_write_byte_identical_files_across_multiple_writes(): void
    {
        $batches = [
            rows(row(int_entry('id', 1)), row(int_entry('id', 2))),
            rows(row(int_entry('id', 3))),
            rows(row(int_entry('id', 4), str_entry('name', 'x')), row(int_entry('id', 5))),
        ];

        $purePath = $this->cacheDir->suffix('multi-write-pure.floe');
        $extPath = $this->cacheDir->suffix('multi-write-ext.floe');

        foreach ([[$purePath, false], [$extPath, true]] as [$path, $useExtension]) {
            $writer = new FloeWriter($this->fs(), useExtension: $useExtension);
            $writer->create($path);

            foreach ($batches as $batch) {
                $writer->write($batch);
            }

            $writer->close();
        }

        static::assertSame($this->fs()->readFrom($purePath)->content(), $this->fs()->readFrom($extPath)->content());
    }

    public function test_extension_and_pure_php_append_byte_identically(): void
    {
        $purePath = $this->cacheDir->suffix('append-pure.floe');
        $extPath = $this->cacheDir->suffix('append-ext.floe');

        foreach ([[$purePath, false], [$extPath, true]] as [$path, $useExtension]) {
            $writer = new FloeWriter($this->fs(), useExtension: $useExtension);
            $writer->create($path);
            $writer->write(rows(row(int_entry('id', 1), str_entry('email', null))));
            $writer->close();

            $writer = new FloeWriter($this->fs(), useExtension: $useExtension);
            $writer->append($path);
            $writer->write(rows(row(int_entry('id', 2), str_entry('email', 'x'))));
            $writer->close();
        }

        static::assertSame($this->fs()->readFrom($purePath)->content(), $this->fs()->readFrom($extPath)->content());
    }
}
