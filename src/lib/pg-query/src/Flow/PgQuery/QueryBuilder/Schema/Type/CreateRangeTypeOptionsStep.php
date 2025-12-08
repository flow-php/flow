<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

interface CreateRangeTypeOptionsStep extends CreateRangeTypeFinalStep
{
    public function canonical(string $function) : self;

    public function collation(string $collation) : self;

    public function multirangeTypeName(string $name) : self;

    public function subtypeDiff(string $function) : self;

    public function subtypeOpclass(string $opclass) : self;
}
