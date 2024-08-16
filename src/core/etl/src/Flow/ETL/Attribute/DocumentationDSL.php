<?php

declare(strict_types=1);

namespace Flow\ETL\Attribute;

#[\Attribute]
final class DocumentationDSL
{
    public function __construct(
        public readonly Module $module,
        public readonly Type $type,
    ) {

    }
}
