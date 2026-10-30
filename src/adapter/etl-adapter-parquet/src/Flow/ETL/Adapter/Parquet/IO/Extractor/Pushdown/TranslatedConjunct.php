<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Extractor\Pushdown;

use Flow\ETL\Values\Schema;
use Flow\Parquet\Filter\Filter;
use Flow\Types\Type\Comparator;

use function Flow\Types\DSL\type_bare;

final readonly class TranslatedConjunct
{
    /**
     * @param list<string> $columns Flow column names the predicate reads
     */
    public function __construct(
        public Filter $filter,
        public array $columns,
    ) {}

    /**
     * True when the file stores every column the predicate reads as $body types it, or not at all. A column the file
     * stores as another type is cast to the body's, so its statistics are of the other type.
     */
    public function readsAs(Schema $fileBody, Schema $body): bool
    {
        $comparator = new Comparator();

        foreach ($this->columns as $column) {
            $stored = $fileBody->findDefinition($column);

            if (
                $stored !== null
                && !$comparator->equals(type_bare($stored->type()), type_bare($body->get($column)->type()))
            ) {
                return false;
            }
        }

        return true;
    }
}
