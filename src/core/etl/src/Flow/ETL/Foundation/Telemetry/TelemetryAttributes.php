<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Telemetry;

/**
 * Custom keys live under `flow.etl.`; official keys come from SemConvAttributes.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class TelemetryAttributes
{
    public const string ATTR_DATAFRAME_ID = 'flow.etl.dataframe.id';

    public const string ATTR_DATAFRAME_NAME = 'flow.etl.dataframe.name';

    public const string ATTR_JOIN_TYPE = 'flow.etl.join.type';

    public const string ATTR_LOADER_CLASS = 'flow.etl.loader.class';

    public const string ATTR_LOADER_DESTINATION_URI = 'flow.etl.destination.uri';

    public const string ATTR_LOADING_ROWS = 'flow.etl.loading.rows';

    /**
     * Value expressed in megabytes.
     */
    public const string ATTR_MEMORY_MAX = 'flow.etl.memory.max';

    /**
     * Value expressed in megabytes.
     */
    public const string ATTR_MEMORY_MIN = 'flow.etl.memory.min';

    public const string ATTR_OPERATOR_CLASS = 'flow.etl.operator.class';

    public const string ATTR_ROWS_THROUGHPUT = 'flow.etl.rows.throughput.per_second';

    public const string ATTR_ROWS_TOTAL = 'flow.etl.rows.total';

    public const string ATTR_SCALAR_FUNCTION = 'flow.etl.scalar.function';

    public const string ATTR_TRANSFORMATION_INPUT_ROWS = 'flow.etl.transformation.input_rows';

    public const string ATTR_TRANSFORMATION_OUTPUT_ROWS = 'flow.etl.transformation.output_rows';

    public const string ATTR_TRANSFORMER_CLASS = 'flow.etl.transformer.class';
}
