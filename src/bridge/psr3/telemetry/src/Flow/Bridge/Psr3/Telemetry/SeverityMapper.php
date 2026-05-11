<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry;

use Flow\Bridge\Psr3\Telemetry\Exception\InvalidArgumentException;
use Flow\Telemetry\Logger\Severity;
use Psr\Log\LogLevel;

final readonly class SeverityMapper
{
    /**
     * @var array<string, Severity>
     */
    private array $mapping;

    /**
     * @param null|array<string, Severity> $customMapping Optional custom mapping (PSR-3 LogLevel string => Telemetry Severity)
     */
    public function __construct(?array $customMapping = null)
    {
        $this->mapping = $customMapping ?? self::defaultMapping();
    }

    /**
     * @return array<string, Severity>
     */
    public static function defaultMapping(): array
    {
        return [
            LogLevel::DEBUG => Severity::DEBUG,
            LogLevel::INFO => Severity::INFO,
            LogLevel::NOTICE => Severity::INFO,
            LogLevel::WARNING => Severity::WARN,
            LogLevel::ERROR => Severity::ERROR,
            LogLevel::CRITICAL => Severity::FATAL,
            LogLevel::ALERT => Severity::FATAL,
            LogLevel::EMERGENCY => Severity::FATAL,
        ];
    }

    /**
     * @throws InvalidArgumentException When the level is not a known PSR-3 LogLevel string
     */
    public function map(string|\Stringable $level): Severity
    {
        $key = (string) $level;

        if (!array_key_exists($key, $this->mapping)) {
            throw new InvalidArgumentException("Unknown PSR-3 log level: {$key}");
        }

        return $this->mapping[$key];
    }
}
