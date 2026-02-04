<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Telemetry;

final class TelemetryAttributes
{
    public const string ATTR_LOADER_DESTINATION_URI = 'destination.uri';

    public const string ATTR_LOADING_ROWS = 'loading.rows';

    public const string ATTR_TRANSFORMATION_INPUT_ROWS = 'transformation.input_rows';

    public const string ATTR_TRANSFORMATION_OUTPUT_ROWS = 'transformation.output_rows';
}
