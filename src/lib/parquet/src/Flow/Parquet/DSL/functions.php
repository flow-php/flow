<?php

declare(strict_types=1);

namespace Flow\Parquet\DSL;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type;
use Flow\Parquet\Filter\ColumnRef;
use Flow\Parquet\Query;

#[DocumentationDSL(module: Module::PARQUET, type: Type::HELPER)]
function parquet_column(string $path): ColumnRef
{
    return new ColumnRef($path);
}

#[DocumentationDSL(module: Module::PARQUET, type: Type::HELPER)]
function parquet_query(): Query
{
    return new Query();
}
