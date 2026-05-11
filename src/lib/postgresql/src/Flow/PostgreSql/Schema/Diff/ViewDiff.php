<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\View;

use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\parsed_select;

final readonly class ViewDiff implements Diff
{
    public function __construct(
        public View $source,
        public View $target,
    ) {}

    /**
     * @return list<Sql>
     */
    public function generate(): array
    {
        return [create()->view($this->target->name)->orReplace()->as(parsed_select($this->target->definition))];
    }
}
