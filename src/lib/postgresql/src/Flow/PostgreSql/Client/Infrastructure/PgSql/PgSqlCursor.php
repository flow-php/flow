<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Infrastructure\PgSql;

use Flow\PostgreSql\Client\{Cursor, RowMapper};
use Flow\PostgreSql\Client\Exception\MappingException;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverters};
use PgSql\Result;

final class PgSqlCursor implements Cursor
{
    /**
     * @var null|list<array{name: string, type: int}>
     */
    private ?array $columnMetaCache = null;

    private int $position = 0;

    public function __construct(private ?Result $result, private readonly ValueConverters $valueConverters, private readonly ?RowMapper $defaultMapper = null)
    {
    }

    /**
     * @return int<0, max>
     */
    public function count() : int
    {
        if ($this->result === null) {
            return 0;
        }

        $count = \pg_num_rows($this->result);

        return $count >= 0 ? $count : 0;
    }

    public function free() : void
    {
        if ($this->result !== null) {
            \pg_free_result($this->result);
            $this->result = null;
        }
    }

    public function getIterator() : \Traversable
    {
        return $this->iterate();
    }

    public function iterate() : \Generator
    {
        while (($row = $this->next()) !== null) {
            yield $row;
        }

        $this->free();
    }

    public function map(string $class, ?RowMapper $mapper = null) : \Generator
    {
        $resolved = $mapper ?? $this->defaultMapper;

        if ($resolved === null) {
            throw MappingException::noMapperConfigured();
        }

        foreach ($this->iterate() as $row) {
            yield $resolved->map($class, $row);
        }
    }

    public function next() : ?array
    {
        if ($this->result === null || $this->position >= \pg_num_rows($this->result)) {
            return null;
        }

        $row = \pg_fetch_assoc($this->result, $this->position);

        if ($row === false) {
            return null;
        }

        $this->position++;

        return $this->convertRow($row);
    }

    /**
     * @return list<array{name: string, type: int}>
     */
    private function columnMeta() : array
    {
        if ($this->columnMetaCache !== null) {
            return $this->columnMetaCache;
        }

        if ($this->result === null) {
            return [];
        }

        $meta = [];
        $count = \pg_num_fields($this->result);

        for ($i = 0; $i < $count; $i++) {
            $meta[] = [
                'name' => \pg_field_name($this->result, $i),
                'type' => (int) \pg_field_type_oid($this->result, $i),
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
    private function convertRow(array $row) : array
    {
        $meta = $this->columnMeta();
        $converted = [];

        $i = 0;

        foreach ($row as $column => $value) {
            $oid = $meta[$i]['type'] ?? 0;
            $type = PostgreSqlType::tryFrom($oid);
            $converted[$column] = $value !== null && $type !== null
                ? $this->valueConverters->forPostgreSqlType($type)->toPhp($value, $type)
                : $value;
            $i++;
        }

        return $converted;
    }
}
