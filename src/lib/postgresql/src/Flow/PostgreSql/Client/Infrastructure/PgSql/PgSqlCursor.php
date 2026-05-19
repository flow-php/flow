<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Infrastructure\PgSql;

use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\Client\RowMapper\Context;
use Flow\PostgreSql\Client\Types\ResultCaster;
use Generator;
use PgSql\Result;
use Traversable;

use function max;
use function pg_fetch_assoc;
use function pg_field_name;
use function pg_field_type;
use function pg_free_result;
use function pg_num_fields;
use function pg_num_rows;

final class PgSqlCursor implements Cursor
{
    /**
     * @var null|list<array{name: string, type: string}>
     */
    private ?array $columnMetaCache = null;

    private int $position = 0;

    private readonly ResultCaster $resultCaster;

    public function __construct(
        private ?Result $result,
        private readonly Context $context,
    ) {
        $this->resultCaster = new ResultCaster();
    }

    /**
     * @return int<0, max>
     */
    public function count(): int
    {
        if ($this->result === null) {
            return 0;
        }

        return max(0, pg_num_rows($this->result));
    }

    public function free(): void
    {
        if ($this->result !== null) {
            pg_free_result($this->result);
            $this->result = null;
        }
    }

    public function getIterator(): Traversable
    {
        return $this->iterate();
    }

    /**
     * @return \Generator<int, array<string, mixed>>
     */
    public function iterate(): Generator
    {
        while (($row = $this->next()) !== null) {
            yield $row;
        }

        $this->free();
    }

    public function map(RowMapper $mapper): Generator
    {
        foreach ($this->iterate() as $row) {
            yield $mapper->map($row, $this->context);
        }
    }

    public function next(): ?array
    {
        if ($this->result === null || $this->position >= pg_num_rows($this->result)) {
            return null;
        }

        $row = pg_fetch_assoc($this->result, $this->position);

        if ($row === false) {
            return null;
        }

        $this->position++;

        return $this->convertRow($row);
    }

    /**
     * @return list<array{name: string, type: string}>
     */
    private function columnMeta(): array
    {
        if ($this->columnMetaCache !== null) {
            return $this->columnMetaCache;
        }

        if ($this->result === null) {
            return [];
        }

        $result = $this->result;
        $meta = [];
        $count = pg_num_fields($result);

        for ($i = 0; $i < $count; $i++) {
            $meta[] = [
                'name' => pg_field_name($result, $i),
                'type' => pg_field_type($result, $i),
            ];
        }

        $this->columnMetaCache = $meta;

        return $meta;
    }

    /**
     * @param array<int|string, null|string> $row
     *
     * @return array<string, mixed>
     */
    private function convertRow(array $row): array
    {
        $meta = $this->columnMeta();
        $converted = [];

        $i = 0;

        foreach ($row as $column => $value) {
            $key = (string) $column;

            if ($value === null) {
                $converted[$key] = null;
            } else {
                $converted[$key] = $this->resultCaster->cast($value, $meta[$i]['type'] ?? null);
            }
            $i++;
        }

        return $converted;
    }
}
