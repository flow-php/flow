<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

interface Hasher
{
    /**
     * @param list<list<mixed>> $values extracted bucket-key values in reference order, one list per row
     *
     * @return list<string> hashes aligned 1:1 with $values
     */
    public function hash(array $values): array;
}
