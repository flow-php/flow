<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\ETL\Values\Schema;

use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

/**
 * Frozen literals of each source's own inference (SourceExtractorTest guards them; ServiceSource arms unguarded).
 */
final readonly class OrdersSchema
{
    public static function of(Source $source): Schema
    {
        return match ($source) {
            Source::csv, Source::excel => schema(
                uuid_schema('order_id', true),
                uuid_schema('seller_id', true),
                datetime_schema('created_at', true),
                datetime_schema('updated_at', true),
                datetime_schema('cancelled_at', true),
                float_schema('discount', true),
                string_schema('email', true),
                string_schema('customer', true),
                json_schema('address', true),
                json_schema('notes', true),
                json_schema('items', true),
                date_schema('ordered_on', true),
            ),
            Source::json, Source::json_lines => schema(
                string_schema('order_id', true),
                string_schema('seller_id', true),
                string_schema('created_at', true),
                string_schema('updated_at', true),
                string_schema('cancelled_at', true),
                float_schema('discount', true),
                string_schema('email', true),
                string_schema('customer', true),
                structure_schema(
                    'address',
                    type_structure([
                        'street' => type_string(),
                        'city' => type_string(),
                        'zip' => type_string(),
                        'country' => type_string(),
                    ]),
                    true,
                ),
                list_schema('notes', type_list(type_string()), true),
                list_schema(
                    'items',
                    type_list(type_structure([
                        'sku' => type_string(),
                        'quantity' => type_integer(),
                        'price' => type_float(),
                    ])),
                    true,
                ),
                string_schema('ordered_on', true),
            ),
            Source::array, Source::floe, Source::memory, Source::parquet => schema(
                uuid_schema('order_id', true),
                uuid_schema('seller_id', true),
                datetime_schema('created_at', true),
                datetime_schema('updated_at', true),
                datetime_schema('cancelled_at', true),
                float_schema('discount', true),
                string_schema('email', true),
                string_schema('customer', true),
                structure_schema(
                    'address',
                    type_structure([
                        'street' => type_string(),
                        'city' => type_string(),
                        'zip' => type_string(),
                        'country' => type_string(),
                    ]),
                    true,
                ),
                list_schema('notes', type_list(type_string()), true),
                list_schema(
                    'items',
                    type_list(type_structure([
                        'sku' => type_string(),
                        'quantity' => type_integer(),
                        'price' => type_float(),
                    ])),
                    true,
                ),
                date_schema('ordered_on', true),
            ),
        };
    }

    /**
     * Exhaustive over ServiceSource on purpose: a third service must make this decision explicitly
     * rather than inherit a shape by default.
     */
    public static function ofService(ServiceSource $source): Schema
    {
        return match ($source) {
            ServiceSource::doctrine, ServiceSource::postgresql => schema(
                uuid_schema('order_id', true),
                uuid_schema('seller_id', true),
                datetime_schema('created_at', true),
                datetime_schema('updated_at', true),
                datetime_schema('cancelled_at', true),
                float_schema('discount', true),
                string_schema('email', true),
                string_schema('customer', true),
                json_schema('address', true),
                json_schema('notes', true),
                json_schema('items', true),
                date_schema('ordered_on', true),
            ),
        };
    }
}
