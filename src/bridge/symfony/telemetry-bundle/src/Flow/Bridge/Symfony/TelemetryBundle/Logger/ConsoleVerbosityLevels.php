<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Logger;

use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Telemetry\Logger\Severity;
use Symfony\Component\Console\Output\OutputInterface;

use function array_key_exists;
use function array_keys;
use function implode;
use function sprintf;

/**
 * Maps a console OutputInterface verbosity to the minimum log {@see Severity} displayed.
 *
 * Translated to Flow's OpenTelemetry severity scale. Flow has no NOTICE level (the
 * PSR-3 bridge already collapses NOTICE into INFO), so - unlike Monolog - there is
 * no rung between WARN and INFO. The defaults therefore keep the terminal quiet by
 * default and reveal exactly one more severity per -v step:
 *
 *   QUIET        => ERROR
 *   NORMAL       => ERROR
 *   VERBOSE  -v  => WARN
 *   VERY..  -vv  => INFO
 *   DEBUG  -vvv  => DEBUG
 *
 * TRACE is never reached by the default ladder; opt into it via verbosity_levels.
 */
final readonly class ConsoleVerbosityLevels
{
    private const array CONSTANT_NAMES = [
        'VERBOSITY_QUIET' => OutputInterface::VERBOSITY_QUIET,
        'VERBOSITY_NORMAL' => OutputInterface::VERBOSITY_NORMAL,
        'VERBOSITY_VERBOSE' => OutputInterface::VERBOSITY_VERBOSE,
        'VERBOSITY_VERY_VERBOSE' => OutputInterface::VERBOSITY_VERY_VERBOSE,
        'VERBOSITY_DEBUG' => OutputInterface::VERBOSITY_DEBUG,
    ];

    /**
     * @param array<int, Severity> $thresholds verbosity constant => minimum severity
     */
    public function __construct(
        private array $thresholds,
    ) {}

    public static function default(): self
    {
        return new self([
            OutputInterface::VERBOSITY_QUIET => Severity::ERROR,
            OutputInterface::VERBOSITY_NORMAL => Severity::ERROR,
            OutputInterface::VERBOSITY_VERBOSE => Severity::WARN,
            OutputInterface::VERBOSITY_VERY_VERBOSE => Severity::INFO,
            OutputInterface::VERBOSITY_DEBUG => Severity::DEBUG,
        ]);
    }

    /**
     * Build from the configured overrides, keyed by Symfony verbosity constant name
     * (e.g. "VERBOSITY_NORMAL") mapped to a Flow severity name (e.g. "WARN").
     *
     * @param array<string, string> $overrides
     */
    public static function fromOverrides(array $overrides): self
    {
        $thresholds = self::default()->thresholds;

        foreach ($overrides as $constantName => $severityName) {
            if (!array_key_exists($constantName, self::CONSTANT_NAMES)) {
                throw new RuntimeException(sprintf(
                    'Unknown console verbosity level "%s"; expected one of: %s.',
                    $constantName,
                    implode(', ', array_keys(self::CONSTANT_NAMES)),
                ));
            }

            $thresholds[self::CONSTANT_NAMES[$constantName]] = self::severityFromName($severityName);
        }

        return new self($thresholds);
    }

    public function thresholdFor(int $verbosity): Severity
    {
        return $this->thresholds[$verbosity] ?? Severity::WARN;
    }

    /**
     * @return array<int, Severity>
     */
    public function thresholds(): array
    {
        return $this->thresholds;
    }

    private static function severityFromName(string $name): Severity
    {
        return match ($name) {
            'TRACE' => Severity::TRACE,
            'DEBUG' => Severity::DEBUG,
            'INFO' => Severity::INFO,
            'WARN' => Severity::WARN,
            'ERROR' => Severity::ERROR,
            'FATAL' => Severity::FATAL,
            default => throw new RuntimeException(sprintf(
                'Unknown severity "%s"; expected one of: TRACE, DEBUG, INFO, WARN, ERROR, FATAL.',
                $name,
            )),
        };
    }
}
