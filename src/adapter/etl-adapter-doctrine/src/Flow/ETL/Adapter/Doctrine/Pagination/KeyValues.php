<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Pagination;

use Flow\ETL\Exception\RuntimeException;

use function array_key_exists;
use function array_map;
use function get_debug_type;
use function implode;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function mb_strlen;
use function mb_substr;
use function sprintf;
use function var_export;

final readonly class KeyValues
{
    /**
     * @param list<bool|float|int|string> $values
     */
    private function __construct(
        private KeySet $keySet,
        public array $values,
    ) {}

    /**
     * @param list<string> $aliases the key_<sha1> alias of every key, by key index
     * @param array<string, mixed> $row
     *
     * @throws RuntimeException
     */
    public static function of(KeySet $keySet, array $aliases, array $row): self
    {
        $values = [];

        foreach ($keySet->keys as $index => $key) {
            if (!array_key_exists($aliases[$index], $row)) {
                throw new RuntimeException(sprintf(
                    'Column "%s" not found in result row for keyset pagination',
                    $key->column,
                ));
            }

            // @mago-expect analysis:mixed-assignment
            $value = $row[$aliases[$index]];

            if ($value === null) {
                throw new RuntimeException(sprintf(
                    'NULL value found in column "%s" for keyset pagination; key columns must be non-null',
                    $key->column,
                ));
            }

            if (!is_string($value) && !is_int($value) && !is_float($value) && !is_bool($value)) {
                throw new RuntimeException(sprintf(
                    'Unsupported value type "%s" in column "%s" for keyset pagination',
                    get_debug_type($value),
                    $key->column,
                ));
            }

            $values[] = $value;
        }

        return new self($keySet, $values);
    }

    public function describe(): string
    {
        return (
            '('
            . implode(', ', array_map(
                static function (Key $key, bool|float|int|string $value): string {
                    $exported = var_export($value, true);

                    return (
                        $key->column
                        . ' = '
                        . (mb_strlen($exported) > 64 ? mb_substr($exported, 0, 64) . '…' : $exported)
                    );
                },
                $this->keySet->keys,
                $this->values,
            ))
            . ')'
        );
    }

    public function equals(self $other): bool
    {
        return $this->values === $other->values;
    }
}
