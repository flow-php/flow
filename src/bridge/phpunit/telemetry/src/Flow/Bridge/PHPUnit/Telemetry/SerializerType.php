<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

enum SerializerType: string
{
    case JSON = 'json';
    case PROTOBUF = 'protobuf';
}
