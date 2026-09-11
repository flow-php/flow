<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;

use function array_map;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function get_debug_type;
use function is_string;
use function sprintf;

final class CacheIndex
{
    /**
     * @var array<string>
     */
    private array $index = [];

    public function __construct(
        public readonly string $key,
    ) {}

    /**
     * @throws InvalidArgumentException
     */
    public static function fromRows(string $key, Rows $rows): self
    {
        $index = new self($key);

        foreach ($rows->all() as $row) {
            $value = $row->get('key');

            if (!is_string($value)) {
                throw new InvalidArgumentException(sprintf(
                    'CacheIndex expects rows with a string "key" entry, got: %s',
                    get_debug_type($value),
                ));
            }

            $index->add($value);
        }

        return $index;
    }

    public function add(string $value): void
    {
        $this->index[] = $value;
    }

    public function toRows(): Rows
    {
        return rows(
            schema(str_schema('key')),
            ...array_map(static fn(string $value): Row => row(['key' => $value]), $this->index),
        );
    }

    /**
     * @return array<string>
     */
    public function values(): array
    {
        return $this->index;
    }
}
