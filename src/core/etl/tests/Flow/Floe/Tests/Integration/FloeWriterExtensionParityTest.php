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

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

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
            'empty' => [rows(schema())],
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
        $rows = rows(schema(datetime_schema('at')), row(['at' => new CustomDateTime('2025-01-01 00:00:00 UTC')]));
        $schema = $rows->schema();

        $writers = [
            'php' => FloeEngineContext::phpWriter($this->fs(), $schema),
            'ext' => FloeEngineContext::nativeWriter($this->fs(), $schema),
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

        $schema = $rows->schema();

        FloeEngineContext::writeAll(FloeEngineContext::phpWriter($this->fs(), $schema), $purePath, [$rows]);
        FloeEngineContext::writeAll(FloeEngineContext::nativeWriter($this->fs(), $schema), $extPath, [$rows]);

        static::assertSame($this->fs()->readFrom($purePath)->content(), $this->fs()->readFrom($extPath)->content());
    }

    public function test_extension_and_pure_php_write_byte_identical_files_across_multiple_writes(): void
    {
        $batches = [
            rows(
                schema(int_schema('id'), str_schema('name')),
                row(['id' => 1, 'name' => 'a']),
                row(['id' => 2, 'name' => 'b']),
            ),
            rows(schema(int_schema('id'), str_schema('name')), row(['id' => 3, 'name' => 'c'])),
            rows(
                schema(int_schema('id'), str_schema('name')),
                row(['id' => 4, 'name' => 'd']),
                row(['id' => 5, 'name' => 'e']),
            ),
        ];

        $purePath = $this->cacheDir->suffix('multi-write-pure.floe');
        $extPath = $this->cacheDir->suffix('multi-write-ext.floe');

        $schema = schema(int_schema('id'), str_schema('name'));

        FloeEngineContext::writeAll(FloeEngineContext::phpWriter($this->fs(), $schema), $purePath, $batches);
        FloeEngineContext::writeAll(FloeEngineContext::nativeWriter($this->fs(), $schema), $extPath, $batches);

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

        $schema = $rows->schema();

        FloeEngineContext::writeAll(FloeEngineContext::phpWriter($this->fs(), $schema, $codec), $purePath, [$rows]);
        FloeEngineContext::writeAll(FloeEngineContext::nativeWriter($this->fs(), $schema, $codec), $extPath, [$rows]);

        static::assertSame($this->fs()->readFrom($purePath)->content(), $this->fs()->readFrom($extPath)->content());
    }

    public function test_extension_and_pure_php_append_byte_identically(): void
    {
        $purePath = $this->cacheDir->suffix('append-pure.floe');
        $extPath = $this->cacheDir->suffix('append-ext.floe');

        $createRows = rows(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            row(['id' => 1, 'email' => null]),
        );
        $appendRows = rows(schema(int_schema('id'), str_schema('email')), row(['id' => 2, 'email' => 'x']));
        $schema = $createRows->schema()->merge($appendRows->schema());

        foreach ([[$purePath, false], [$extPath, true]] as [$path, $native]) {
            $create = $native
                ? FloeEngineContext::nativeWriter($this->fs(), $schema)
                : FloeEngineContext::phpWriter($this->fs(), $schema);
            $create->create($path);
            $create->write($createRows);
            $create->close();

            $append = $native
                ? FloeEngineContext::nativeWriter($this->fs(), $schema)
                : FloeEngineContext::phpWriter($this->fs(), $schema);
            $append->append($path);
            $append->write($appendRows);
            $append->close();
        }

        static::assertSame($this->fs()->readFrom($purePath)->content(), $this->fs()->readFrom($extPath)->content());
    }
}
