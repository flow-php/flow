<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

use Flow\ETL\Exception\InvalidArgumentException;

use function assert;
use function implode;
use function is_string;
use function sprintf;
use function str_repeat;
use function str_split;
use function strlen;

final readonly class FixedBinary16Layout implements Layout
{
    public function encode(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            assert($physical === null || is_string($physical));
            $values[] = $physical ?? str_repeat("\0", 16);
        }

        return [implode('', $values)];
    }

    public function decode(Buffers $buffers, int $count, array $valid): array
    {
        $buffer = $buffers->next();

        if (strlen($buffer) !== (16 * $count)) {
            throw new InvalidArgumentException(sprintf(
                'FixedBinary16 values buffer of %d bytes, expected %d for %d rows',
                strlen($buffer),
                16 * $count,
                $count,
            ));
        }

        if ($count === 0) {
            return [];
        }

        $physicals = [];

        foreach (str_split($buffer, 16) as $i => $value) {
            $physicals[] = $valid[$i] ? $value : null;
        }

        return $physicals;
    }
}
