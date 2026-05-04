<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Context;

use Google\Protobuf\Internal\Message;
use Grpc\BaseStub;
use Opentelemetry\Proto\Collector\Trace\V1\TraceServiceClient;
use PHPUnit\Framework\TestCase;

final class Requirements
{
    public static function requireGrpc() : void
    {
        if (!\extension_loaded('grpc')) {
            TestCase::markTestSkipped('The grpc extension is not available');
        }

        if (!\class_exists(BaseStub::class)) {
            TestCase::markTestSkipped('The grpc/grpc package is not installed');
        }

        self::requireProtobuf();
    }

    public static function requireProtobuf() : void
    {
        if (!\class_exists(Message::class)) {
            TestCase::markTestSkipped('The google/protobuf package is not installed');
        }

        if (!\class_exists(TraceServiceClient::class)) {
            TestCase::markTestSkipped('The open-telemetry/gen-otlp-protobuf package is not installed');
        }
    }
}
