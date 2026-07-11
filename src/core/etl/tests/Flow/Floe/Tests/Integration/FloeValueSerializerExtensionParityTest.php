<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Fixtures\CustomDateTime;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FloeValueSerializer;
use Flow\Floe\Tests\Mother\RowsMother;
use Override;
use PHPUnit\Framework\Attributes\DataProvider;

use function extension_loaded;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

/**
 * The bulk codec encodes/decodes the frames region natively when flow_php is loaded. These tests pin
 * the pure-PHP path as the canonical reference: the native encode must be BYTE-IDENTICAL and the
 * native decode graph-identical.
 */
final class FloeValueSerializerExtensionParityTest extends FlowIntegrationTestCase
{
    public static function value_datasets(): array
    {
        return [
            'all entry types' => [RowsMother::withAllEntryTypes()],
            'heterogeneous' => [RowsMother::heterogeneous()],
            'partitioned' => [RowsMother::partitioned()],
            'empty' => [rows()],
            'single row' => [row(int_entry('id', 1))],
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

    #[DataProvider('value_datasets')]
    public function test_extension_and_pure_php_decode_identically(Row|Rows $value): void
    {
        $bytes = (new FloeValueSerializer(useExtension: false))->encode($value);

        static::assertEquals(
            (new FloeValueSerializer(useExtension: false))->decode($bytes),
            (new FloeValueSerializer(useExtension: true))->decode($bytes),
        );
    }

    #[DataProvider('value_datasets')]
    public function test_extension_encodes_byte_identical_to_pure_php(Row|Rows $value): void
    {
        static::assertSame(
            (new FloeValueSerializer(useExtension: false))->encode($value),
            (new FloeValueSerializer(useExtension: true))->encode($value),
        );
    }

    public function test_extension_decode_failure_is_wrapped_as_floe_exception(): void
    {
        $bytes = (new FloeValueSerializer(useExtension: false))->encode(rows(row(int_entry('id', 1))));
        // corrupt the first frame type (SCHEMA -> unknown 0x7F); the footer at the end stays intact
        $bytes[6] = "\x7F";

        $this->expectException(FloeException::class);

        (new FloeValueSerializer(useExtension: true))->decode($bytes);
    }

    public function test_extension_encode_failure_is_wrapped_as_floe_exception(): void
    {
        $this->expectException(FloeException::class);

        (new FloeValueSerializer(useExtension: true))->encode(rows(row(datetime_entry(
            'd',
            new CustomDateTime('2025-01-01 00:00:00'),
        ))));
    }

    #[DataProvider('value_datasets')]
    public function test_streaming_mode_decodes_identically_on_both_engines(Row|Rows $value): void
    {
        $bytes = (new FloeValueSerializer(useExtension: false))->encode($value);

        static::assertEquals(
            (new FloeValueSerializer(2, useExtension: false))->decode($bytes),
            (new FloeValueSerializer(2, useExtension: true))->decode($bytes),
        );
    }

    #[DataProvider('value_datasets')]
    public function test_streaming_mode_encodes_byte_identical_on_both_engines_and_to_bulk(Row|Rows $value): void
    {
        $bulk = (new FloeValueSerializer(useExtension: true))->encode($value);

        static::assertSame($bulk, (new FloeValueSerializer(2, useExtension: false))->encode($value));
        static::assertSame($bulk, (new FloeValueSerializer(2, useExtension: true))->encode($value));
    }
}
