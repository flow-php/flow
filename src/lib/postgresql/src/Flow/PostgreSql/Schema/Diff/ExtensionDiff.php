<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use function Flow\PostgreSql\DSL\alter;

use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Extension;

final readonly class ExtensionDiff implements Diff
{
    public function __construct(
        public Extension $source,
        public Extension $target,
    ) {
    }

    /**
     * @return list<Sql>
     */
    public function generate() : array
    {
        if (!$this->hasVersionChanged()) {
            return [];
        }

        if ($this->target->version === null) {
            return [alter()->extension($this->target->name)->update()];
        }

        return [alter()->extension($this->target->name)->updateTo($this->target->version)];
    }

    public function hasVersionChanged() : bool
    {
        return $this->source->version !== $this->target->version;
    }
}
