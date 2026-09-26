<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\NativeFloeEncoder;
use Flow\Floe\Tests\Context\FloeEngineContext;
use Flow\Floe\Tests\Mother\RowsMother;

use function array_values;
use function Flow\Filesystem\DSL\path;

/**
 * The fixture is a 0x02 file whose footer still carries "allow_extra": false on its structure column - byte for byte,
 * it cannot be regenerated once normalize() stopped writing the key.
 */
final class FloePreAllowExtraRemovalTest extends FlowIntegrationTestCase
{
    public function test_the_fixture_footer_still_carries_allow_extra(): void
    {
        static::assertStringContainsString(
            '"allow_extra":false',
            $this->fs()->readFrom(path(__DIR__
            . '/../Fixtures/pre-allow-extra-removal/all-entry-types.floe'))->content(),
        );
    }

    public function test_php_reader_reads_a_footer_that_carries_allow_extra(): void
    {
        static::assertEquals(
            array_values(RowsMother::withAllEntryTypes()->all()),
            FloeEngineContext::readRows(
                FloeEngineContext::phpReader($this->fs()),
                path(__DIR__ . '/../Fixtures/pre-allow-extra-removal/all-entry-types.floe'),
            ),
        );
    }

    public function test_native_reader_reads_a_footer_that_carries_allow_extra(): void
    {
        if (!NativeFloeEncoder::isSupported()) {
            static::markTestSkipped('flow_php extension with the RawRowValues pipeline is not loaded.');
        }

        static::assertEquals(
            array_values(RowsMother::withAllEntryTypes()->all()),
            FloeEngineContext::readRows(
                FloeEngineContext::nativeReader($this->fs()),
                path(__DIR__ . '/../Fixtures/pre-allow-extra-removal/all-entry-types.floe'),
            ),
        );
    }
}
