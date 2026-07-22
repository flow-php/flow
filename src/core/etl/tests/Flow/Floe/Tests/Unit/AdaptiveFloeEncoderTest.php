<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\Floe\AdaptiveFloeEncoder;
use Flow\Floe\Tests\Context\FloeSchemaContext;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\str_entry;

final class AdaptiveFloeEncoderTest extends TestCase
{
    public function test_encode_decode_round_trips_through_the_selected_engine(): void
    {
        $data = rows(
            row(int_entry('id', 1), str_entry('name', 'flow')),
            row(int_entry('id', 2), str_entry('name', null)),
        );
        $encoder = new AdaptiveFloeEncoder(schema_from_json(FloeSchemaContext::schemaBody($data->first()->schema())));

        $decoded = $encoder->decode($encoder->encode((new PhpRowHydrator())->dehydrate($data)));

        static::assertEquals(
            [new RawRowValues(['id' => 1, 'name' => 'flow']), new RawRowValues(['id' => 2, 'name' => null])],
            $decoded,
        );
    }
}
