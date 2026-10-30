<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\IO\Loader;

use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\TextValues;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;

final readonly class DbalEncoder
{
    public function __construct(
        private TextValues $text = new TextValues(),
    ) {}

    /**
     * @return list<array<string, mixed>>
     */
    public function encode(Rows $rows): array
    {
        $columns = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $name = $definition->name();
            $columns[$name] = $this->column($definition->type(), $rows->column($name));
        }

        $normalized = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $row = [];

            foreach ($columns as $name => $column) {
                $row[$name] = $column[$i];
            }

            $normalized[] = $row;
        }

        return $normalized;
    }

    /**
     * @param Type<mixed> $type
     *
     * @return list<mixed>
     */
    public function column(Type $type, Column $column): array
    {
        return $this->text->bindable($type, $column);
    }
}
