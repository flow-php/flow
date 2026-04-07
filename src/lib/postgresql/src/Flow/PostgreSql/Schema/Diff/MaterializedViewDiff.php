<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use function Flow\PostgreSql\DSL\{create, drop};

use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexMethod as QbIndexMethod;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\{Index, IndexMethod, MaterializedView};

final readonly class MaterializedViewDiff implements Diff
{
    /**
     * @param list<Index> $addedIndexes
     * @param list<Index> $removedIndexes
     */
    public function __construct(
        public MaterializedView $source,
        public MaterializedView $target,
        public array $addedIndexes = [],
        public array $removedIndexes = [],
    ) {
    }

    /**
     * @return list<Sql>
     */
    public function generate() : array
    {
        $sqls = [];

        if ($this->hasDefinitionChanged()) {
            foreach ($this->source->indexes as $idx) {
                $sqls[] = drop()->index($idx->name);
            }

            $sqls[] = drop()->materializedView($this->source->name);
            $sqls = [...$sqls, ...$this->target->toSql()];

            return $sqls;
        }

        foreach ($this->removedIndexes as $idx) {
            $sqls[] = drop()->index($idx->name);
        }

        foreach ($this->addedIndexes as $idx) {
            $builder = create()->index($idx->name);

            if ($idx->unique) {
                $builder = $builder->unique();
            }

            $onBuilder = $builder->on($this->target->name);

            if ($idx->method !== IndexMethod::BTREE) {
                $onBuilder = $onBuilder->using(QbIndexMethod::from($idx->method->value));
            }

            $sqls[] = $onBuilder->columns(...$idx->columns);
        }

        return $sqls;
    }

    public function hasDefinitionChanged() : bool
    {
        return $this->source->definition !== $this->target->definition;
    }
}
