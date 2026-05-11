<?php

declare(strict_types=1);

namespace Flow\PostgreSql;

final class NamedParameterNormalizer
{
    /**
     * Extract named parameter names from SQL in the order they appear.
     *
     * @return array<string, int> Map of parameter names to their positional index (1-based)
     */
    public function extractParameters(string $sql): array
    {
        $parameters = [];
        $position = 1;

        preg_replace_callback(
            '/(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)/',
            static function (array $matches) use (&$parameters, &$position): string {
                $name = $matches[1];

                if (!\array_key_exists($name, $parameters)) {
                    $parameters[$name] = $position++;
                }

                return '';
            },
            $sql,
        );

        return $parameters;
    }

    /**
     * Convert named parameters (e.g., :id, :name) to PostgreSQL positional parameters ($1, $2, etc.).
     */
    public function normalize(string $sql): string
    {
        $parameters = [];
        $position = 1;

        $converted = preg_replace_callback(
            '/(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)/',
            static function (array $matches) use (&$parameters, &$position): string {
                $name = $matches[1];

                if (!\array_key_exists($name, $parameters)) {
                    $parameters[$name] = $position++;
                }

                return '$' . $parameters[$name];
            },
            $sql,
        );

        return $converted ?? $sql;
    }
}
