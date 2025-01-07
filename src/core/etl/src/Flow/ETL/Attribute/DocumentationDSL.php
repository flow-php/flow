<?php

declare(strict_types=1);

namespace Flow\ETL\Attribute;

#[\Attribute]
final readonly class DocumentationDSL
{
    public function __construct(
        public Module $module,
        public Type $type,
    ) {

    }
}
