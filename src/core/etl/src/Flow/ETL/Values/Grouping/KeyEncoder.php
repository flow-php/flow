<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Grouping;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\CanonicalEncoding;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\ValueEquality;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Types\Type;

use function hash;
use function hexdec;
use function substr;

final readonly class KeyEncoder
{
    /**
     * @param list<string> $columns
     * @param array<int, Type<mixed>> $types by position: the type a column is cast to before it is encoded; the
     *                                       declared type when a position has none
     * @param null|Schema $declared the bound schema of the batches, when the caller has one
     */
    public function __construct(
        private array $columns,
        private array $types = [],
        private ?Schema $declared = null,
    ) {}

    /**
     * @return list<Column> the key columns of $rows, cast as of() casts them
     */
    public function columns(Rows $rows, Backend $backend): array
    {
        $equality = new ValueEquality();
        $columns = [];

        foreach ($this->columns as $position => $name) {
            $declared = $this->declared?->findDefinition($name);

            if ($rows->schema()->findDefinition($name) === null && $declared?->isNullable() === true) {
                $columns[] = $backend->constant($declared, null, $rows->count());

                continue;
            }

            $type = $this->types[$position] ?? $declared?->type();
            // Rows::column() refuses a column the batch lacks, naming it
            $columns[] = $type === null ? $rows->column($name) : $equality->cast($rows->column($name), $type, $name);
        }

        return $columns;
    }

    /**
     * @return list<string> one key per row
     */
    public function of(Rows $rows): array
    {
        return (
            $rows->isEmpty()
                ? []
                : (new CanonicalEncoding())->ordered($this->columns($rows, new PhpBackend()), $rows->count())
        );
    }

    /**
     * @param list<Column> $columns
     *
     * @return list<string> one key per row, the keys of() gives for the same columns
     */
    public static function ofColumns(array $columns, int $count): array
    {
        return $count < 1 ? [] : (new CanonicalEncoding())->ordered($columns, $count);
    }

    public static function bucketOf(string $key, int $buckets, int $hashOffset): int
    {
        return (int) hexdec(substr(hash('xxh128', $key), $hashOffset, 8)) % $buckets;
    }
}
