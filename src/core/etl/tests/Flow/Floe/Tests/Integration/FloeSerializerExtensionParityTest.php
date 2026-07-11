<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\FloeSerializer;
use Flow\Floe\Tests\Mother\RowsMother;
use Override;
use PHPUnit\Framework\Attributes\DataProvider;

use function extension_loaded;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

/**
 * With the flow_php extension loaded, FloeSerializer::unserialize hydrates ROW frame bodies
 * through the extension. These tests pin the pure-PHP hydrator as the canonical reference and
 * assert the extension produces an identical object graph.
 */
final class FloeSerializerExtensionParityTest extends FlowIntegrationTestCase
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
    public function test_extension_and_pure_php_unserialize_identical_value(Row|Rows $value): void
    {
        $serialized = (new FloeSerializer())->serialize($value);

        static::assertEquals(
            (new FloeSerializer(useExtension: false))->unserialize($serialized, [Row::class, Rows::class]),
            (new FloeSerializer(useExtension: true))->unserialize($serialized, [Row::class, Rows::class]),
        );
    }
}
