<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration;

use CmsIg\Seal\Schema\Schema;
use Flow\ETL\Adapter\Seal\Tests\SealTestCase;
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;

use function Flow\ETL\Adapter\Seal\to_seal;
use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\DSL\flow_context;

final class SealAllEntryTypesTest extends SealTestCase
{
    public function test_round_trip_of_all_flow_entry_types(): void
    {
        $engine = $this->sealContext()->engine();

        to_seal($engine, self::INDEX_NAME)->load((new FakeStaticOrdersExtractor(5))->toRows(), flow_context());

        static::assertSame(5, $engine->countDocuments(self::INDEX_NAME));

        $document = $engine->getDocument(self::INDEX_NAME, '0');

        static::assertSame('user-0@example.com', $document['email']);

        /** @var array<string, mixed> $address */
        $address = $document['address'];
        static::assertSame('123 Main St, Apt 0', $address['street']);

        /** @var list<mixed> $notes */
        $notes = $document['notes'];
        static::assertCount(3, $notes);

        /** @var list<array<string, mixed>> $items */
        $items = $document['items'];
        static::assertSame('SKU_0001', $items[0]['sku']);
        static::assertSame(1, $items[0]['quantity']);
    }

    protected function schema(): Schema
    {
        return to_seal_schema(FakeStaticOrdersExtractor::schema(), self::INDEX_NAME, 'index');
    }
}
