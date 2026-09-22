<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;

use function array_map;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function get_debug_type;
use function is_int;
use function is_string;
use function sprintf;

final class CacheIndex
{
    /**
     * @var list<array{key: string, rows: null|int}>
     */
    private array $chunks = [];

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

            // an index written by 0.44.x has no "rows" column
            $chunkRows = $row->has('rows') ? $row->get('rows') : null;

            if ($chunkRows !== null && !is_int($chunkRows)) {
                throw new InvalidArgumentException(sprintf(
                    'CacheIndex expects rows with an integer "rows" entry, got: %s',
                    get_debug_type($chunkRows),
                ));
            }

            $index->add($value, $chunkRows);
        }

        return $index;
    }

    public function add(string $value, ?int $rows = null): void
    {
        $this->chunks[] = ['key' => $value, 'rows' => $rows];
    }

    public function rows(): Cardinality
    {
        $total = 0;

        foreach ($this->chunks as $chunk) {
            if ($chunk['rows'] === null) {
                return Cardinality::unknown();
            }

            $total += $chunk['rows'];
        }

        return Cardinality::exact($total);
    }

    public function toRows(): Rows
    {
        return rows(
            schema(str_schema('key'), int_schema('rows', nullable: true)),
            ...array_map(static fn(array $chunk): Row => row($chunk), $this->chunks),
        );
    }

    /**
     * @return list<string>
     */
    public function values(): array
    {
        return array_map(static fn(array $chunk): string => $chunk['key'], $this->chunks);
    }
}
