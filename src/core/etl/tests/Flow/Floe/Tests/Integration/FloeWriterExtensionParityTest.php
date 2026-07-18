<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Rows;
use Flow\ETL\Tests\Fixtures\CustomDateTime;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\NativeFloeEncoder;
use Flow\Floe\Tests\Context\FloeEngineContext;
use Flow\Floe\Tests\Double\PrefixingCodecStub;
use Flow\Floe\Tests\Mother\RowsMother;
use Override;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

/**
 * With the flow_php extension loaded, FloeWriter encodes ROW frame bodies through
 * NativeFloeEncoder. These tests pin PhpFloeEncoder as the canonical reference and
 * assert the native engine writes byte-identical files.
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

        if (!NativeFloeEncoder::isSupported()) {
            self::markTestSkipped('flow_php extension is not loaded.');
        }
    }

    public function test_extension_and_pure_php_reject_datetime_subclass_identically(): void
    {
        $rows = rows(row(datetime_entry('at', new CustomDateTime('2025-01-01 00:00:00 UTC'))));

        $writers = [
            'php' => FloeEngineContext::phpWriter($this->fs()),
            'ext' => FloeEngineContext::nativeWriter($this->fs()),
        ];

        foreach ($writers as $engine => $writer) {
            $writer->create($this->cacheDir->suffix('subclass-' . $engine . '.floe'));

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

        FloeEngineContext::writeAll(FloeEngineContext::phpWriter($this->fs()), $purePath, [$rows]);
        FloeEngineContext::writeAll(FloeEngineContext::nativeWriter($this->fs()), $extPath, [$rows]);

        static::assertSame($this->fs()->readFrom($purePath)->content(), $this->fs()->readFrom($extPath)->content());
    }

    public function test_extension_and_pure_php_write_byte_identical_files_across_multiple_writes(): void
    {
        $batches = [
            rows(row(int_entry('id', 1), str_entry('name', 'a')), row(int_entry('id', 2), str_entry('name', 'b'))),
            rows(row(int_entry('id', 3), str_entry('name', 'c'))),
            rows(row(int_entry('id', 4), str_entry('name', 'd')), row(int_entry('id', 5), str_entry('name', 'e'))),
        ];

        $purePath = $this->cacheDir->suffix('multi-write-pure.floe');
        $extPath = $this->cacheDir->suffix('multi-write-ext.floe');

        FloeEngineContext::writeAll(FloeEngineContext::phpWriter($this->fs()), $purePath, $batches);
        FloeEngineContext::writeAll(FloeEngineContext::nativeWriter($this->fs()), $extPath, $batches);

        static::assertSame($this->fs()->readFrom($purePath)->content(), $this->fs()->readFrom($extPath)->content());
    }

    /**
     * A transforming (non-identity) codec must not fork the engine: the native
     * body-encode produces the same ROW bodies as the PHP engine, the codec runs
     * in PHP either way, so the files stay byte-identical (the A1 fix).
     */
    #[DataProvider('rows_datasets')]
    public function test_extension_and_pure_php_write_byte_identical_files_with_transforming_codec(Rows $rows): void
    {
        $codec = new PrefixingCodecStub();

        $purePath = $this->cacheDir->suffix('write-codec-pure.floe');
        $extPath = $this->cacheDir->suffix('write-codec-ext.floe');

        FloeEngineContext::writeAll(FloeEngineContext::phpWriter($this->fs(), $codec), $purePath, [$rows]);
        FloeEngineContext::writeAll(FloeEngineContext::nativeWriter($this->fs(), $codec), $extPath, [$rows]);

        static::assertSame($this->fs()->readFrom($purePath)->content(), $this->fs()->readFrom($extPath)->content());
    }

    public function test_extension_and_pure_php_append_byte_identically(): void
    {
        $purePath = $this->cacheDir->suffix('append-pure.floe');
        $extPath = $this->cacheDir->suffix('append-ext.floe');

        foreach ([[$purePath, false], [$extPath, true]] as [$path, $native]) {
            $create = $native
                ? FloeEngineContext::nativeWriter($this->fs())
                : FloeEngineContext::phpWriter($this->fs());
            $create->create($path);
            $create->write(rows(row(int_entry('id', 1), str_entry('email', null))));
            $create->close();

            $append = $native
                ? FloeEngineContext::nativeWriter($this->fs())
                : FloeEngineContext::phpWriter($this->fs());
            $append->append($path);
            $append->write(rows(row(int_entry('id', 2), str_entry('email', 'x'))));
            $append->close();
        }

        static::assertSame($this->fs()->readFrom($purePath)->content(), $this->fs()->readFrom($extPath)->content());
    }
}
