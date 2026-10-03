<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

interface Layout
{
    /**
     * @param list<mixed> $physicals nulls allowed
     *
     * @return list<string> value buffers, null slots in canonical form
     */
    public function encode(array $physicals): array;

    /**
     * @param list<bool> $valid
     *
     * @return list<mixed> physicals, null where !$valid[$i]
     */
    public function decode(Buffers $buffers, int $count, array $valid): array;
}
