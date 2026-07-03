<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

use DateTimeInterface;

use function array_combine;
use function array_keys;
use function array_map;
use function array_slice;
use function is_array;
use function is_bool;
use function is_object;
use function is_scalar;
use function json_encode;
use function mb_strlen;
use function mb_substr;
use function method_exists;

/**
 * Formats bound statement parameters into `db.query.parameter.<key>` span attributes.
 */
final readonly class ParameterFormatter
{
    /**
     * @param array<int|string, mixed> $parameters
     *
     * @return array<string, string>
     */
    public function format(array $parameters, int $maxParameters, int $maxParameterLength): array
    {
        $slice = array_slice($parameters, 0, $maxParameters, true);

        if ($slice === []) {
            return [];
        }

        return array_combine(
            array_map(
                static fn(int|string $key): string => DbAttributes::DB_QUERY_PARAMETER_PREFIX . $key,
                array_keys($slice),
            ),
            array_map(fn(mixed $value): string => $this->truncate(
                $this->convertToString($value),
                $maxParameterLength,
            ), $slice),
        );
    }

    private function convertToString(mixed $value): string
    {
        if ($value === null) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (is_scalar($value)) {
            return (string) $value;
        }

        if ($value instanceof DateTimeInterface) {
            return $value->format('c');
        }

        if (is_array($value)) {
            return json_encode($value, JSON_THROW_ON_ERROR);
        }

        if (is_object($value) && method_exists($value, '__toString')) {
            return (string) $value;
        }

        return json_encode($value, JSON_THROW_ON_ERROR);
    }

    private function truncate(string $value, int $maxLength): string
    {
        if ($maxLength <= 0 || mb_strlen($value) <= $maxLength) {
            return $value;
        }

        return mb_substr($value, 0, $maxLength) . '...';
    }
}
