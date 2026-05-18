<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use Flow\Telemetry\ErrorHandler\ErrorLogMessageType;
use Flow\Telemetry\ErrorHandler\SyslogFacility;
use Flow\Telemetry\ErrorHandler\SyslogSeverity;
use InvalidArgumentException;
use PHPUnit\Runner\Extension\ParameterCollection;

use function array_diff;
use function array_merge;
use function array_unique;
use function array_values;
use function ctype_digit;
use function explode;
use function getenv;
use function implode;
use function in_array;
use function intval;
use function is_string;
use function sprintf;
use function str_contains;
use function str_starts_with;
use function strtolower;
use function strtoupper;
use function trigger_error;
use function trim;
use function urldecode;

use const E_USER_DEPRECATED;
use const LOG_PID;

final readonly class Configuration
{
    public const int DEFAULT_BATCH_SIZE = 512;

    public const int DEFAULT_CONNECT_TIMEOUT_MS = 250;

    public const string DEFAULT_ENDPOINT = 'http://localhost:4318';

    public const int DEFAULT_FILE_PERMISSIONS = 0644;

    public const string DEFAULT_MESSAGE_PREFIX = '[flow-telemetry]';

    public const int DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;

    public const int DEFAULT_TIMEOUT_MS = 250;

    public const string ERROR_HANDLER_LOG = 'error_log';

    public const string ERROR_HANDLER_NOOP = 'noop';

    public const string ERROR_HANDLER_STREAM = 'stream';

    public const string ERROR_HANDLER_SYSLOG = 'syslog';

    public const string ERROR_HANDLER_UDP_SYSLOG = 'udp_syslog';

    public const string TRANSPORT_CURL = 'curl';

    public const string TRANSPORT_GRPC = 'grpc';

    public const string TRANSPORT_STREAM = 'stream';

    private const array CURL_SPECIFIC_PARAMS = [
        'curl_timeout_ms',
        'curl_connect_timeout_ms',
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

    private const array ERROR_LOG_HANDLER_PARAMS = [
        'error_handler_message_type',
        'error_handler_expand_newlines',
        'error_handler_message_prefix',
    ];

    private const array GRPC_SPECIFIC_PARAMS = [
        'grpc_insecure',
        'grpc_timeout_ms',
    ];

    private const array SHARED_PARAMS = [
        'transport',
        'endpoint',
        'headers',
        'shutdown_timeout_ms',
    ];

    private const array STREAM_HANDLER_PARAMS = [
        'error_handler_destination',
        'error_handler_file_permissions',
        'error_handler_create_directories',
        'error_handler_message_prefix',
    ];

    private const array STREAM_TRANSPORT_PARAMS = [
        'stream_file_permissions',
        'stream_create_directories',
    ];

    private const array SYSLOG_HANDLER_PARAMS = [
        'error_handler_ident',
        'error_handler_facility',
        'error_handler_log_opts',
        'error_handler_severity',
    ];

    private const array UDP_SYSLOG_HANDLER_PARAMS = [
        'error_handler_host',
        'error_handler_port',
        'error_handler_ident',
        'error_handler_facility',
        'error_handler_severity',
    ];

    public function __construct(
        public string $serviceName,
        public CurlTransportConfig|GrpcTransportConfig|StreamTransportConfig $transport,
        public bool $emitTraces,
        public bool $emitMetrics,
        public bool $emitTestSpans,
        public bool $emitTestCaseSpans,
        public int $batchSize,
        public ErrorLogHandlerConfig|NullErrorHandlerConfig|StreamErrorHandlerConfig|SyslogErrorHandlerConfig|UdpSyslogErrorHandlerConfig $errorHandler,
    ) {}

    public static function fromParameters(ParameterCollection $parameters): self
    {
        $legacyUrl = self::resolveLegacyCollectorUrl($parameters);

        if ($legacyUrl !== null) {
            self::rejectParams(
                $parameters,
                array_merge(
                    self::SHARED_PARAMS,
                    self::CURL_SPECIFIC_PARAMS,
                    self::GRPC_SPECIFIC_PARAMS,
                    self::STREAM_TRANSPORT_PARAMS,
                ),
                'Deprecated parameter "otel_collector_url" cannot be mixed with new parameter "%s", migrate fully to the new parameter shape.',
            );
            @trigger_error('Parameter "otel_collector_url" is deprecated, use "endpoint" instead.', E_USER_DEPRECATED);
        }

        return new self(
            serviceName: self::resolve($parameters, 'service_name') ?? 'phpunit',
            transport: self::resolveTransport($parameters, $legacyUrl),
            emitTraces: self::resolveBool($parameters, 'emit_traces', true),
            emitMetrics: self::resolveBool($parameters, 'emit_metrics', true),
            emitTestSpans: self::resolveBool($parameters, 'emit_test_spans', true),
            emitTestCaseSpans: self::resolveBool($parameters, 'emit_test_case_spans', true),
            batchSize: self::resolveInt($parameters, 'batch_size', self::DEFAULT_BATCH_SIZE),
            errorHandler: self::resolveErrorHandler($parameters),
        );
    }

    private static function buildErrorLogHandlerConfig(ParameterCollection $parameters): ErrorLogHandlerConfig
    {
        self::rejectForeignHandlerParams($parameters, self::ERROR_HANDLER_LOG, self::ERROR_LOG_HANDLER_PARAMS);

        return new ErrorLogHandlerConfig(
            messageType: self::resolveErrorLogMessageType($parameters),
            expandNewlines: self::resolveBool($parameters, 'error_handler_expand_newlines', false),
            messagePrefix: self::resolve($parameters, 'error_handler_message_prefix') ?? self::DEFAULT_MESSAGE_PREFIX,
        );
    }

    private static function buildNullErrorHandlerConfig(ParameterCollection $parameters): NullErrorHandlerConfig
    {
        self::rejectForeignHandlerParams($parameters, self::ERROR_HANDLER_NOOP, []);

        return new NullErrorHandlerConfig();
    }

    private static function buildStreamErrorHandlerConfig(ParameterCollection $parameters): StreamErrorHandlerConfig
    {
        self::rejectForeignHandlerParams($parameters, self::ERROR_HANDLER_STREAM, self::STREAM_HANDLER_PARAMS);

        $destination = self::resolve($parameters, 'error_handler_destination');

        if ($destination === null || $destination === '') {
            throw new InvalidArgumentException(
                'Parameter "error_handler_destination" is required for error_handler "stream".',
            );
        }

        return new StreamErrorHandlerConfig(
            destination: $destination,
            filePermissions: self::resolveFilePermissions($parameters, 'error_handler_file_permissions'),
            createDirectories: self::resolveBool($parameters, 'error_handler_create_directories', true),
            messagePrefix: self::resolve($parameters, 'error_handler_message_prefix') ?? self::DEFAULT_MESSAGE_PREFIX,
        );
    }

    private static function buildSyslogErrorHandlerConfig(ParameterCollection $parameters): SyslogErrorHandlerConfig
    {
        self::rejectForeignHandlerParams($parameters, self::ERROR_HANDLER_SYSLOG, self::SYSLOG_HANDLER_PARAMS);

        return new SyslogErrorHandlerConfig(
            ident: self::resolve($parameters, 'error_handler_ident') ?? 'flow-telemetry',
            facility: self::resolveSyslogFacility($parameters),
            logOpts: self::resolveInt($parameters, 'error_handler_log_opts', LOG_PID),
            severity: self::resolveSyslogSeverity($parameters),
        );
    }

    private static function buildUdpSyslogErrorHandlerConfig(ParameterCollection $parameters): UdpSyslogErrorHandlerConfig
    {
        self::rejectForeignHandlerParams($parameters, self::ERROR_HANDLER_UDP_SYSLOG, self::UDP_SYSLOG_HANDLER_PARAMS);

        $host = self::resolve($parameters, 'error_handler_host');

        if ($host === null || $host === '') {
            throw new InvalidArgumentException(
                'Parameter "error_handler_host" is required for error_handler "udp_syslog".',
            );
        }

        return new UdpSyslogErrorHandlerConfig(
            host: $host,
            port: self::resolveInt($parameters, 'error_handler_port', 514),
            ident: self::resolve($parameters, 'error_handler_ident') ?? 'flow-telemetry',
            facility: self::resolveSyslogFacility($parameters),
            severity: self::resolveSyslogSeverity($parameters),
        );
    }

    /**
     * @return array<string, string>
     */
    private static function parseHeaders(string $raw): array
    {
        if ($raw === '') {
            return [];
        }

        $headers = [];

        foreach (explode(',', $raw) as $entry) {
            $entry = trim($entry);

            if ($entry === '') {
                continue;
            }

            if (!str_contains($entry, '=')) {
                throw new InvalidArgumentException(sprintf(
                    'Invalid header entry "%s", expected format "name=value".',
                    $entry,
                ));
            }

            [$name, $value] = explode('=', $entry, 2);
            $name = urldecode(trim($name));

            if ($name === '') {
                throw new InvalidArgumentException('Header name cannot be empty.');
            }

            $headers[$name] = urldecode($value);
        }

        return $headers;
    }

    private static function readEnv(string $fullName): ?string
    {
        $value = $_ENV[$fullName] ?? $_SERVER[$fullName] ?? getenv($fullName);

        if (!is_string($value) || $value === '') {
            return null;
        }

        return $value;
    }

    /**
     * @param list<string> $allowedParams
     */
    private static function rejectForeignHandlerParams(
        ParameterCollection $parameters,
        string $type,
        array $allowedParams,
    ): void {
        $foreign = array_values(array_diff(
            array_values(array_unique(array_merge(
                self::ERROR_LOG_HANDLER_PARAMS,
                self::STREAM_HANDLER_PARAMS,
                self::SYSLOG_HANDLER_PARAMS,
                self::UDP_SYSLOG_HANDLER_PARAMS,
            ))),
            $allowedParams,
        ));
        self::rejectParams(
            $parameters,
            $foreign,
            sprintf('Parameter "%%s" cannot be used with error_handler "%s".', $type),
        );
    }

    /**
     * @param list<string> $forbidden
     */
    private static function rejectParams(
        ParameterCollection $parameters,
        array $forbidden,
        string $messageTemplate,
    ): void {
        foreach ($forbidden as $name) {
            if (self::resolve($parameters, $name) !== null) {
                throw new InvalidArgumentException(sprintf($messageTemplate, $name));
            }
        }
    }

    private static function resolve(ParameterCollection $parameters, string $name): ?string
    {
        $env = self::readEnv(self::ENV_PREFIX . strtoupper($name));

        if ($env !== null) {
            return $env;
        }

        if ($parameters->has($name)) {
            return $parameters->get($name);
        }

        return null;
    }

    private static function resolveBool(ParameterCollection $parameters, string $name, bool $default): bool
    {
        $value = self::resolve($parameters, $name);

        if ($value === null) {
            return $default;
        }

        $lower = strtolower($value);

        if ($lower === 'true' || $lower === '1') {
            return true;
        }

        if ($lower === 'false' || $lower === '0') {
            return false;
        }

        throw new InvalidArgumentException(sprintf(
            'Invalid boolean value "%s" for parameter "%s", expected "true" or "false".',
            $value,
            $name,
        ));
    }

    private static function resolveErrorHandler(ParameterCollection $parameters): ErrorLogHandlerConfig|NullErrorHandlerConfig|StreamErrorHandlerConfig|SyslogErrorHandlerConfig|UdpSyslogErrorHandlerConfig
    {
        $type = self::resolve($parameters, 'error_handler') ?? self::ERROR_HANDLER_LOG;

        return match ($type) {
            self::ERROR_HANDLER_LOG => self::buildErrorLogHandlerConfig($parameters),
            self::ERROR_HANDLER_NOOP => self::buildNullErrorHandlerConfig($parameters),
            self::ERROR_HANDLER_STREAM => self::buildStreamErrorHandlerConfig($parameters),
            self::ERROR_HANDLER_SYSLOG => self::buildSyslogErrorHandlerConfig($parameters),
            self::ERROR_HANDLER_UDP_SYSLOG => self::buildUdpSyslogErrorHandlerConfig($parameters),
            default => throw new InvalidArgumentException(sprintf(
                'Invalid error_handler "%s", expected one of: %s.',
                $type,
                implode(', ', [
                    self::ERROR_HANDLER_LOG,
                    self::ERROR_HANDLER_NOOP,
                    self::ERROR_HANDLER_STREAM,
                    self::ERROR_HANDLER_SYSLOG,
                    self::ERROR_HANDLER_UDP_SYSLOG,
                ]),
            )),
        };
    }

    private static function resolveErrorLogMessageType(ParameterCollection $parameters): ErrorLogMessageType
    {
        $value = self::resolve($parameters, 'error_handler_message_type');

        if ($value === null) {
            return ErrorLogMessageType::OperatingSystem;
        }

        return match ($value) {
            'operating_system' => ErrorLogMessageType::OperatingSystem,
            'email' => ErrorLogMessageType::Email,
            'file' => ErrorLogMessageType::File,
            'sapi' => ErrorLogMessageType::Sapi,
            default => throw new InvalidArgumentException(sprintf(
                'Invalid error_handler_message_type "%s", expected "operating_system", "email", "file" or "sapi".',
                $value,
            )),
        };
    }

    private static function resolveFilePermissions(ParameterCollection $parameters, string $name): int
    {
        $raw = self::resolve($parameters, $name);

        if ($raw === null) {
            return self::DEFAULT_FILE_PERMISSIONS;
        }

        // Allow octal literals like "0640" by interpreting strings starting with "0" as octal.
        $value = str_starts_with($raw, '0') && ctype_digit($raw)
            ? intval($raw, 8)
            : self::resolveInt($parameters, $name, self::DEFAULT_FILE_PERMISSIONS);

        if ($value < 0 || $value > 0o777) {
            throw new InvalidArgumentException(sprintf(
                'Invalid file_permissions value "%s" for parameter "%s", expected octal between 0 and 0777.',
                $raw,
                $name,
            ));
        }

        return $value;
    }

    private static function resolveInt(ParameterCollection $parameters, string $name, int $default): int
    {
        $value = self::resolve($parameters, $name);

        if ($value === null) {
            return $default;
        }

        if (!ctype_digit($value)) {
            throw new InvalidArgumentException(sprintf(
                'Invalid integer value "%s" for parameter "%s", expected a non-negative integer.',
                $value,
                $name,
            ));
        }

        return (int) $value;
    }

    private static function resolveLegacyCollectorUrl(ParameterCollection $parameters): ?string
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

    private static function resolveSerializer(ParameterCollection $parameters): SerializerType
    {
        $value = self::resolve($parameters, 'curl_serializer');

        if ($value === null) {
            return SerializerType::JSON;
        }

        $serializer = SerializerType::tryFrom($value);

        if ($serializer === null) {
            throw new InvalidArgumentException(sprintf(
                'Invalid serializer "%s" for parameter "curl_serializer", expected "json" or "protobuf".',
                $value,
            ));
        }

        return $serializer;
    }

    private static function resolveSyslogFacility(ParameterCollection $parameters): SyslogFacility
    {
        $value = self::resolve($parameters, 'error_handler_facility');

        if ($value === null) {
            return SyslogFacility::User;
        }

        return match ($value) {
            'auth' => SyslogFacility::Auth,
            'cron' => SyslogFacility::Cron,
            'daemon' => SyslogFacility::Daemon,
            'kernel' => SyslogFacility::Kernel,
            'local0' => SyslogFacility::Local0,
            'local1' => SyslogFacility::Local1,
            'local2' => SyslogFacility::Local2,
            'local3' => SyslogFacility::Local3,
            'local4' => SyslogFacility::Local4,
            'local5' => SyslogFacility::Local5,
            'local6' => SyslogFacility::Local6,
            'local7' => SyslogFacility::Local7,
            'lpr' => SyslogFacility::Lpr,
            'mail' => SyslogFacility::Mail,
            'news' => SyslogFacility::News,
            'syslog' => SyslogFacility::Syslog,
            'user' => SyslogFacility::User,
            'uucp' => SyslogFacility::Uucp,
            default => throw new InvalidArgumentException(sprintf('Invalid error_handler_facility "%s".', $value)),
        };
    }

    private static function resolveSyslogSeverity(ParameterCollection $parameters): SyslogSeverity
    {
        $value = self::resolve($parameters, 'error_handler_severity');

        if ($value === null) {
            return SyslogSeverity::Error;
        }

        return match ($value) {
            'alert' => SyslogSeverity::Alert,
            'critical' => SyslogSeverity::Critical,
            'debug' => SyslogSeverity::Debug,
            'emergency' => SyslogSeverity::Emergency,
            'error' => SyslogSeverity::Error,
            'info' => SyslogSeverity::Info,
            'notice' => SyslogSeverity::Notice,
            'warning' => SyslogSeverity::Warning,
            default => throw new InvalidArgumentException(sprintf('Invalid error_handler_severity "%s".', $value)),
        };
    }

    private static function resolveTransport(
        ParameterCollection $parameters,
        ?string $legacyUrl,
    ): CurlTransportConfig|GrpcTransportConfig|StreamTransportConfig {
        $transportType = self::resolve($parameters, 'transport') ?? self::TRANSPORT_CURL;

        if (!in_array($transportType, [self::TRANSPORT_CURL, self::TRANSPORT_GRPC, self::TRANSPORT_STREAM], true)) {
            throw new InvalidArgumentException(sprintf(
                'Invalid transport "%s", expected "%s", "%s" or "%s".',
                $transportType,
                self::TRANSPORT_CURL,
                self::TRANSPORT_GRPC,
                self::TRANSPORT_STREAM,
            ));
        }

        $endpoint = $legacyUrl ?? self::resolve($parameters, 'endpoint') ?? self::DEFAULT_ENDPOINT;

        $headers = self::parseHeaders(self::resolve($parameters, 'headers') ?? '');

        if ($transportType === self::TRANSPORT_CURL) {
            self::rejectParams(
                $parameters,
                self::GRPC_SPECIFIC_PARAMS,
                'Parameter "%s" cannot be used with transport "curl".',
            );
            self::rejectParams(
                $parameters,
                self::STREAM_TRANSPORT_PARAMS,
                'Parameter "%s" cannot be used with transport "curl".',
            );

            return new CurlTransportConfig(
                endpoint: $endpoint,
                headers: $headers,
                timeoutMs: self::resolveInt($parameters, 'curl_timeout_ms', self::DEFAULT_TIMEOUT_MS),
                connectTimeoutMs: self::resolveInt(
                    $parameters,
                    'curl_connect_timeout_ms',
                    self::DEFAULT_CONNECT_TIMEOUT_MS,
                ),
                shutdownTimeoutMs: self::resolveInt(
                    $parameters,
                    'shutdown_timeout_ms',
                    self::DEFAULT_SHUTDOWN_TIMEOUT_MS,
                ),
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
        }

        if ($transportType === self::TRANSPORT_GRPC) {
            self::rejectParams(
                $parameters,
                self::CURL_SPECIFIC_PARAMS,
                'Parameter "%s" cannot be used with transport "grpc".',
            );
            self::rejectParams(
                $parameters,
                self::STREAM_TRANSPORT_PARAMS,
                'Parameter "%s" cannot be used with transport "grpc".',
            );

            return new GrpcTransportConfig(
                endpoint: $endpoint,
                headers: $headers,
                insecure: self::resolveBool($parameters, 'grpc_insecure', true),
                timeoutMs: self::resolveInt($parameters, 'grpc_timeout_ms', self::DEFAULT_TIMEOUT_MS),
                shutdownTimeoutMs: self::resolveInt(
                    $parameters,
                    'shutdown_timeout_ms',
                    self::DEFAULT_SHUTDOWN_TIMEOUT_MS,
                ),
            );
        }

        self::rejectParams(
            $parameters,
            self::CURL_SPECIFIC_PARAMS,
            'Parameter "%s" cannot be used with transport "stream".',
        );
        self::rejectParams(
            $parameters,
            self::GRPC_SPECIFIC_PARAMS,
            'Parameter "%s" cannot be used with transport "stream".',
        );
        self::rejectParams(
            $parameters,
            ['headers', 'shutdown_timeout_ms'],
            'Parameter "%s" cannot be used with transport "stream".',
        );

        $rawEndpoint = self::resolve($parameters, 'endpoint');

        if ($rawEndpoint === null || $rawEndpoint === '') {
            throw new InvalidArgumentException(
                'Parameter "endpoint" is required for transport "stream" (file path or php:// stream wrapper URI).',
            );
        }

        return new StreamTransportConfig(
            destination: $rawEndpoint,
            filePermissions: self::resolveFilePermissions($parameters, 'stream_file_permissions'),
            createDirectories: self::resolveBool($parameters, 'stream_create_directories', true),
        );
    }
}
