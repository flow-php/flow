<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

/**
 * Flow-specific filesystem attribute keys.
 *
 * No official filesystem semantic convention exists; per OTel naming guidance all custom keys
 * live under the `flow.filesystem.` prefix. Official keys (e.g. `error.type`) come from
 * {@see \Flow\Telemetry\SemConvAttributes}.
 *
 * @see https://opentelemetry.io/docs/specs/semconv/general/naming/
 */
final class FilesystemTelemetryAttributes
{
    public const string ATTR_BYTES_TOTAL_READ = 'flow.filesystem.bytes.total_read';

    public const string ATTR_BYTES_TOTAL_WRITTEN = 'flow.filesystem.bytes.total_written';

    public const string ATTR_FILESYSTEM_OPERATION = 'flow.filesystem.operation';

    public const string ATTR_FILESYSTEM_PROTOCOL = 'flow.filesystem.protocol';

    public const string ATTR_PATH_TO = 'flow.filesystem.path.to';

    public const string ATTR_PATH_URI = 'flow.filesystem.path.uri';

    public const string ATTR_STREAM_TYPE = 'flow.filesystem.stream.type';
}
