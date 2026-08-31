<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use DOMDocument;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;

use function assert;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;
use function serialize;
use function unserialize;

final class RowsSerializationTest extends FlowTestCase
{
    public function test_native_columns_survive_a_serialize_round_trip(): void
    {
        $rows = rows(
            schema(int_schema('id'), str_schema('name', nullable: true)),
            row(['id' => 1, 'name' => 'a']),
            row(['id' => 2, 'name' => null]),
        );

        /** @var Rows $restored */
        $restored = unserialize(serialize($rows));

        static::assertSame([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]], $restored->toArray());
        static::assertTrue($rows->schema()->isSame($restored->schema()));
    }

    public function test_xml_column_metadata_survives_a_serialize_round_trip(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<a>1</a>');

        $rows = rows(
            schema(xml_schema('x', metadata: Metadata::empty()->add('src', 'file.xml'))),
            row(['x' => $document]),
        );

        /** @var Rows $restored */
        $restored = unserialize(serialize($rows));
        $restoredDocument = $restored->first()->get('x');
        assert($restoredDocument instanceof DOMDocument);

        static::assertSame(['src' => 'file.xml'], $restored->schema()->get('x')->metadata()->normalize());
        static::assertSame($rows->schema()->get('x')->isNullable(), $restored->schema()->get('x')->isNullable());
        static::assertSame($document->C14N(), $restoredDocument->C14N());
    }
}
