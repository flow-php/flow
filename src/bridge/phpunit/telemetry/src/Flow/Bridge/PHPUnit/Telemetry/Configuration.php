<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use PHPUnit\Runner\Extension\ParameterCollection;

final readonly class Configuration
{
    public const string DEFAULT_ENDPOINT = 'http://localhost:4318';

    private const array CURL_SPECIFIC_PARAMS = [
        'curl_timeout',
        'curl_connect_timeout',
        'curl_compression',
        'curl_follow_redirects',
        'curl_max_redirects',
        'curl_proxy',
        'curl_ssl_verify_peer',
        'curl_ssl_verify_host',
        'curl_ssl_cert_path',
        'curl_ssl_key_path',
        'curl_ca_info_path',
        'curl_serializer',
    ];

    private const string ENV_PREFIX = 'FLOW_PHPUNIT_OTEL_';

    private const array GRPC_SPECIFIC_PARAMS = [
        'grpc_insecure',
    ];

    private const array SHARED_PARAMS = [
        'transport',
        'endpoint',
        'headers',
    ];

    public function __construct(
        public string $serviceName,
        public CurlTransportConfig|GrpcTransportConfig $transport,
        public bool $emitTraces,
        public bool $emitMetrics,
        public bool $emitTestSpans,
        public bool $emitTestCaseSpans,
    ) {
    }

    public static function fromParameters(ParameterCollection $parameters) : self
    {
        $legacyUrl = self::resolveLegacyCollectorUrl($parameters);

        if ($legacyUrl !== null) {
            self::rejectParams(
                $parameters,
                \array_merge(self::SHARED_PARAMS, self::CURL_SPECIFIC_PARAMS, self::GRPC_SPECIFIC_PARAMS),
                'Deprecated parameter "otel_collector_url" cannot be mixed with new parameter "%s", migrate fully to the new parameter shape.',
            );
            @\trigger_error(
                'Parameter "otel_collector_url" is deprecated, use "endpoint" instead.',
                \E_USER_DEPRECATED,
            );
        }

        $transportType = self::resolve($parameters, 'transport') ?? 'curl';

        if (!\in_array($transportType, ['curl', 'grpc'], true)) {
            throw new \InvalidArgumentException(\sprintf(
                'Invalid transport "%s", expected "curl" or "grpc".',
                $transportType,
            ));
        }

        $endpoint = $legacyUrl
            ?? self::resolve($parameters, 'endpoint')
            ?? self::DEFAULT_ENDPOINT;

        $headers = self::parseHeaders(self::resolve($parameters, 'headers') ?? '');

        if ($transportType === 'curl') {
            self::rejectParams($parameters, self::GRPC_SPECIFIC_PARAMS, 'Parameter "%s" cannot be used with transport "curl".');

            $transport = new CurlTransportConfig(
                endpoint: $endpoint,
                headers: $headers,
                timeout: self::resolveInt($parameters, 'curl_timeout', 30),
                connectTimeout: self::resolveInt($parameters, 'curl_connect_timeout', 10),
                compression: self::resolveBool($parameters, 'curl_compression', false),
                followRedirects: self::resolveBool($parameters, 'curl_follow_redirects', true),
                maxRedirects: self::resolveInt($parameters, 'curl_max_redirects', 3),
                proxy: self::resolve($parameters, 'curl_proxy'),
                sslVerifyPeer: self::resolveBool($parameters, 'curl_ssl_verify_peer', true),
                sslVerifyHost: self::resolveBool($parameters, 'curl_ssl_verify_host', true),
                sslCertPath: self::resolve($parameters, 'curl_ssl_cert_path'),
                sslKeyPath: self::resolve($parameters, 'curl_ssl_key_path'),
                caInfoPath: self::resolve($parameters, 'curl_ca_info_path'),
                serializer: self::resolveSerializer($parameters),
            );
        } else {
            self::rejectParams($parameters, self::CURL_SPECIFIC_PARAMS, 'Parameter "%s" cannot be used with transport "grpc".');

            $transport = new GrpcTransportConfig(
                endpoint: $endpoint,
                headers: $headers,
                insecure: self::resolveBool($parameters, 'grpc_insecure', true),
            );
        }

        return new self(
            serviceName: self::resolve($parameters, 'service_name') ?? 'phpunit',
            transport: $transport,
            emitTraces: self::resolveBool($parameters, 'emit_traces', true),
            emitMetrics: self::resolveBool($parameters, 'emit_metrics', true),
            emitTestSpans: self::resolveBool($parameters, 'emit_test_spans', true),
            emitTestCaseSpans: self::resolveBool($parameters, 'emit_test_case_spans', true),
        );
    }

    /**
     * @return array<string, string>
     */
    private static function parseHeaders(string $raw) : array
    {
        if ($raw === '') {
            return [];
        }

        $headers = [];

        foreach (\explode(',', $raw) as $entry) {
            $entry = \trim($entry);

            if ($entry === '') {
                continue;
            }

            if (!\str_contains($entry, '=')) {
                throw new \InvalidArgumentException(\sprintf(
                    'Invalid header entry "%s", expected format "name=value".',
                    $entry,
                ));
            }

            [$name, $value] = \explode('=', $entry, 2);
            $name = \urldecode(\trim($name));

            if ($name === '') {
                throw new \InvalidArgumentException('Header name cannot be empty.');
            }

            $headers[$name] = \urldecode($value);
        }

        return $headers;
    }

    private static function readEnv(string $fullName) : ?string
    {
        $value = $_ENV[$fullName] ?? $_SERVER[$fullName] ?? \getenv($fullName);

        if (!\is_string($value) || $value === '') {
            return null;
        }

        return $value;
    }

    /**
     * @param list<string> $forbidden
     */
    private static function rejectParams(ParameterCollection $parameters, array $forbidden, string $messageTemplate) : void
    {
        foreach ($forbidden as $name) {
            if (self::resolve($parameters, $name) !== null) {
                throw new \InvalidArgumentException(\sprintf($messageTemplate, $name));
            }
        }
    }

    private static function resolve(ParameterCollection $parameters, string $name) : ?string
    {
        $env = self::readEnv(self::ENV_PREFIX . \strtoupper($name));

        if ($env !== null) {
            return $env;
        }

        if ($parameters->has($name)) {
            return $parameters->get($name);
        }

        return null;
    }

    private static function resolveBool(ParameterCollection $parameters, string $name, bool $default) : bool
    {
        $value = self::resolve($parameters, $name);

        if ($value === null) {
            return $default;
        }

        $lower = \strtolower($value);

        if ($lower === 'true' || $lower === '1') {
            return true;
        }

        if ($lower === 'false' || $lower === '0') {
            return false;
        }

        throw new \InvalidArgumentException(\sprintf(
            'Invalid boolean value "%s" for parameter "%s", expected "true" or "false".',
            $value,
            $name,
        ));
    }

    private static function resolveInt(ParameterCollection $parameters, string $name, int $default) : int
    {
        $value = self::resolve($parameters, $name);

        if ($value === null) {
            return $default;
        }

        if (!\ctype_digit($value)) {
            throw new \InvalidArgumentException(\sprintf(
                'Invalid integer value "%s" for parameter "%s", expected a non-negative integer.',
                $value,
                $name,
            ));
        }

        return (int) $value;
    }

    private static function resolveLegacyCollectorUrl(ParameterCollection $parameters) : ?string
    {
        $env = self::readEnv(self::ENV_PREFIX . 'COLLECTOR_URL');

        if ($env !== null) {
            return $env;
        }

        if ($parameters->has('otel_collector_url')) {
            return $parameters->get('otel_collector_url');
        }

        return null;
    }

    private static function resolveSerializer(ParameterCollection $parameters) : SerializerType
    {
        $value = self::resolve($parameters, 'curl_serializer');

        if ($value === null) {
            return SerializerType::JSON;
        }

        $serializer = SerializerType::tryFrom($value);

        if ($serializer === null) {
            throw new \InvalidArgumentException(\sprintf(
                'Invalid serializer "%s" for parameter "curl_serializer", expected "json" or "protobuf".',
                $value,
            ));
        }

        return $serializer;
    }
}
