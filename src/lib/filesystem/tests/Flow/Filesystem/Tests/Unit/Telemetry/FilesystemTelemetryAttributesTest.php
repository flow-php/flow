<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use Flow\Filesystem\Telemetry\FilesystemTelemetryAttributes;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

final class FilesystemTelemetryAttributesTest extends TestCase
{
    public function test_all_custom_keys_use_the_flow_filesystem_prefix(): void
    {
        // @mago-expect analysis:mixed-assignment
        foreach ((new ReflectionClass(FilesystemTelemetryAttributes::class))->getConstants() as $name => $value) {
            static::assertIsString($value);
            static::assertStringStartsWith(
                'flow.filesystem.',
                $value,
                "Constant {$name} must not extend a reserved OTel namespace",
            );
        }
    }

    public function test_bytes_attributes_are_defined(): void
    {
        static::assertSame('flow.filesystem.bytes.total_read', FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ);
        static::assertSame(
            'flow.filesystem.bytes.total_written',
            FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN,
        );
    }

    public function test_operation_attributes_are_defined(): void
    {
        static::assertSame('flow.filesystem.operation', FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION);
        static::assertSame('flow.filesystem.protocol', FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL);
        static::assertSame('flow.filesystem.stream.type', FilesystemTelemetryAttributes::ATTR_STREAM_TYPE);
    }

    public function test_path_attributes_are_defined(): void
    {
        static::assertSame('flow.filesystem.path.to', FilesystemTelemetryAttributes::ATTR_PATH_TO);
        static::assertSame('flow.filesystem.path.uri', FilesystemTelemetryAttributes::ATTR_PATH_URI);
    }
}
