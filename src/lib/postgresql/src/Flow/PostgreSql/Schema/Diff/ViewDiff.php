<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use function Flow\PostgreSql\DSL\{create, parsed_select};

use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\View;

final readonly class ViewDiff implements Diff
{
    public function __construct(
        public View $source,
        public View $target,
    ) {
    }

    /**
     * @return list<Sql>
     */
    public function generate() : array
    {
        return [create()->view($this->target->name)->orReplace()->as(parsed_select($this->target->definition))];
    }
}
