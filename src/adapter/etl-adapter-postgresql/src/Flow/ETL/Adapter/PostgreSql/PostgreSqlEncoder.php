<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\TextValues;
use Flow\ETL\Rows;
use Flow\Types\Type;

final readonly class PostgreSqlEncoder
{
    public function __construct(
        private TextValues $text = new TextValues(),
    ) {}

    /**
     * @param Type<mixed> $type
     *
     * @return list<mixed>
     */
    public function column(Type $type, Column $column): array
    {
        return $this->text->bindable($type, $column);
    }

    /**
     * Every column's values, in schema order, as `column()` gives them.
     *
     * @return array<string, list<mixed>>
     */
    public function columns(Rows $rows): array
    {
        $columns = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $name = $definition->entry()->name();
            $columns[$name] = $this->column($definition->type(), $rows->column($name));
        }

        return $columns;
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function encode(Rows $rows): array
    {
        $columns = $this->columns($rows);
        $encoded = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $row = [];

            foreach ($columns as $name => $column) {
                $row[$name] = $column[$i];
            }

            $encoded[] = $row;
        }

        return $encoded;
    }
}
