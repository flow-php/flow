<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

final class FilesystemTelemetryAttributes
{
    public const string ATTR_BYTES_READ = 'bytes.read';

    public const string ATTR_BYTES_WRITTEN = 'bytes.written';

    public const string ATTR_FILESYSTEM_OPERATION = 'filesystem.operation';

    public const string ATTR_FILESYSTEM_PROTOCOL = 'filesystem.protocol';

    public const string ATTR_PATH_FROM = 'path.from';

    public const string ATTR_PATH_IS_PATTERN = 'path.is_pattern';

    public const string ATTR_PATH_TO = 'path.to';

    public const string ATTR_PATH_URI = 'path.uri';

    public const string ATTR_STREAM_TYPE = 'stream.type';
}
