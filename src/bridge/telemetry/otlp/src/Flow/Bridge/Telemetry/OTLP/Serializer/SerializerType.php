<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Serializer;

enum SerializerType: string
{
    case JSON = 'json';
    case PROTOBUF = 'protobuf';
}
