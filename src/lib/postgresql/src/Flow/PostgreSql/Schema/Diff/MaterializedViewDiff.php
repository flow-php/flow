<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Index;
use Flow\PostgreSql\Schema\MaterializedView;

use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\drop;

final readonly class MaterializedViewDiff implements Diff
{
    /**
     * @param list<Index> $addedIndexes
     * @param list<Index> $removedIndexes
     * @param array<string, Index> $renamedIndexes
     */
    public function __construct(
        public MaterializedView $source,
        public MaterializedView $target,
        public array $addedIndexes = [],
        public array $removedIndexes = [],
        public array $renamedIndexes = [],
    ) {}

    /**
     * @return list<Sql>
     */
    public function generate(): array
    {
        $sqls = [];

        if ($this->hasDefinitionChanged()) {
            foreach ($this->source->indexes as $idx) {
                $sqls[] = drop()->index($this->source->schemaName() . '.' . $idx->name);
            }

            $sqls[] = drop()->materializedView($this->source->qualifiedName());
            return [...$sqls, ...$this->target->toSql()];
        }

        foreach ($this->removedIndexes as $idx) {
            $sqls[] = drop()->index($this->source->schemaName() . '.' . $idx->name);
        }

        foreach ($this->addedIndexes as $idx) {
            $sqls[] = $idx->toSql($this->target->name, $this->target->schemaName());
        }

        foreach ($this->renamedIndexes as $oldName => $newIndex) {
            $sqls[] = alter()->index($oldName, $this->source->schemaName())->renameTo($newIndex->name);
        }

        return $sqls;
    }

    public function hasDefinitionChanged(): bool
    {
        return $this->source->definition !== $this->target->definition;
    }
}
