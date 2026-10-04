<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

final readonly class NullCount
{
    /**
     * @param list<mixed> $values
     */
    public function of(array $values): int
    {
        $nulls = 0;

        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $value) {
            if ($value === null) {
                $nulls++;
            }
        }

        return $nulls;
    }
}
