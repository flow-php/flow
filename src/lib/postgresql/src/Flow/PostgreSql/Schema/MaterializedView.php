<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use function Flow\PostgreSql\DSL\{create, parsed_select};

use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexMethod as QbIndexMethod;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

final readonly class MaterializedView
{
    /**
     * @param list<Index> $indexes
     */
    public function __construct(
        public string $name,
        public string $definition,
        public array $indexes = [],
    ) {
    }

    /**
     * @return list<SqlQuery>
     */
    public function toSql() : array
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
