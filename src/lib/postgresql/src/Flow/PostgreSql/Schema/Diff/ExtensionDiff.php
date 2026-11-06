<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Extension;

use function Flow\PostgreSql\DSL\alter;
use function Flow\Types\DSL\type_string;

final readonly class ExtensionDiff implements Diff
{
    public function __construct(
        public Extension $source,
        public Extension $target,
    ) {}

    /**
     * @return list<Sql>
     */
    public function generate(): array
    {
        $sqls = [];

        if ($this->hasSchemaChanged()) {
            $sqls[] = alter()->extension($this->target->name)->setSchema($this->target->schema);
        }

        if ($this->hasVersionChanged()) {
            $sqls[] = alter()->extension($this->target->name)->updateTo(type_string()->assert($this->target->version));
        }

        return $sqls;
    }

    public function hasSchemaChanged(): bool
    {
        return $this->source->schema !== $this->target->schema;
    }

    public function hasVersionChanged(): bool
    {
        return $this->target->version !== null && $this->source->version !== $this->target->version;
    }
}
