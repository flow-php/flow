<?php

declare(strict_types=1);

namespace Flow\ETL\Attribute;

#[\Attribute]
final class DSL
{
    public function __construct(
        public readonly Module $module,
        public readonly Type $type,
    ) {

    }
}
