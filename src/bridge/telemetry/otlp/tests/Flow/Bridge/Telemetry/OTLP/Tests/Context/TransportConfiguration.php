<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Context;

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_transport, otlp_grpc_transport, otlp_http_transport, otlp_json_serializer, otlp_protobuf_serializer};
use Flow\Bridge\Telemetry\OTLP\Serializer\ProtobufSerializer;
use Flow\Telemetry\Transport\Transport;
use Google\Protobuf\Internal\Message;

final readonly class TransportConfiguration
{
    private function __construct(
        public string $name,
        public string $transport,
        public string $serializer,
    ) {
    }

    /**
     * @return list<self>
     */
    public static function all() : array
    {
        return [
            self::httpJson(),
            self::httpProtobuf(),
            self::curlJson(),
            self::curlProtobuf(),
            self::grpcProtobuf(),
        ];
    }

    /**
     * @return list<self>
     */
    public static function available() : array
    {
        return \array_values(\array_filter(self::all(), static fn (self $config) => $config->isAvailable()));
    }

    public static function curlJson() : self
    {
        return new self('curl-json', 'curl', 'json');
    }

    public static function curlProtobuf() : self
    {
        return new self('curl-protobuf', 'curl', 'protobuf');
    }

    public static function grpcProtobuf() : self
    {
        return new self('grpc-protobuf', 'grpc', 'protobuf');
    }

    public static function httpJson() : self
    {
        return new self('http-json', 'http', 'json');
    }

    public static function httpProtobuf() : self
    {
        return new self('http-protobuf', 'http', 'protobuf');
    }

    public function createTransport(OtelContext $ctx) : Transport
    {
        $serializer = match ($this->serializer) {
            'json' => otlp_json_serializer(),
            'protobuf' => otlp_protobuf_serializer(),
            default => throw new \InvalidArgumentException(\sprintf('Unknown serializer: %s', $this->serializer)),
        };

        return match ($this->transport) {
            'http' => otlp_http_transport(
                $ctx->httpClient(),
                $ctx->requestFactory(),
                $ctx->streamFactory(),
                $ctx->httpEndpoint(),
                $serializer,
            ),
            'curl' => otlp_curl_transport(
                $ctx->httpEndpoint(),
                $serializer,
            ),
            'grpc' => otlp_grpc_transport(
                $ctx->grpcEndpoint(),
                $serializer instanceof ProtobufSerializer ? $serializer : otlp_protobuf_serializer(),
            ),
            default => throw new \InvalidArgumentException(\sprintf('Unknown transport: %s', $this->transport)),
        };
    }

    public function isAvailable() : bool
    {
        if ($this->transport === 'grpc' && !\extension_loaded('grpc')) {
            return false;
        }

        if ($this->serializer === 'protobuf' && !\class_exists(Message::class)) {
            return false;
        }

        return true;
    }
}
