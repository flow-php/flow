<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexMethod as QbIndexMethod;
use Flow\PostgreSql\QueryBuilder\Sql;

use function array_map;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\parsed_select;

/**
 * @import-type IndexShape from Index
 *
 * @type MaterializedViewShape = array{name: string, definition: string, indexes: list<IndexShape>}
 */
final readonly class MaterializedView
{
    /**
     * @param list<Index> $indexes
     */
    public function __construct(
        public string $name,
        public string $definition,
        public array $indexes = [],
    ) {}

    /**
     * @param MaterializedViewShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(name: $data['name'], definition: $data['definition'], indexes: array_map(
            static fn(array $index): Index => Index::fromArray($index),
            $data['indexes'],
        ));
    }

    /**
     * @return MaterializedViewShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'definition' => $this->definition,
            'indexes' => array_map(static fn(Index $index): array => $index->normalize(), $this->indexes),
        ];
    }

    /**
     * @return list<Sql>
     */
    public function toSql(): array
    {
        $sqls = [];

        $sqls[] = create()->materializedView($this->name)->as(parsed_select($this->definition));

        foreach ($this->indexes as $idx) {
            $builder = create()->index($idx->name);

            if ($idx->unique) {
                $builder = $builder->unique();
            }

            $onBuilder = $builder->on($this->name);

            if ($idx->method !== IndexMethod::BTREE) {
                $onBuilder = $onBuilder->using(QbIndexMethod::from($idx->method->value));
            }

            $sqls[] = $onBuilder->columns(...$idx->columns);
        }

        return $sqls;
    }
}
