<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\Floe\AdaptiveFloeEncoder;
use Flow\Floe\Tests\Context\FloeSchemaContext;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_schema;

final class AdaptiveFloeEncoderTest extends TestCase
{
    public function test_encode_decode_round_trips_through_the_selected_engine(): void
    {
        $data = rows(
            schema(int_schema('id'), str_schema('name', nullable: true)),
            row(['id' => 1, 'name' => 'flow']),
            row(['id' => 2, 'name' => null]),
        );
        $encoder = new AdaptiveFloeEncoder(schema_from_json(FloeSchemaContext::schemaBody($data->schema())));

        $decoded = $encoder->decode($encoder->encode((new PhpRowHydrator())->dehydrate($data)));

        static::assertEquals(
            [new RawRowValues(['id' => 1, 'name' => 'flow']), new RawRowValues(['id' => 2, 'name' => null])],
            $decoded,
        );
    }
}
