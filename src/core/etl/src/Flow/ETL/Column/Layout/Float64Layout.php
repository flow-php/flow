<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

use Flow\ETL\Exception\InvalidArgumentException;

use function array_values;
use function pack;
use function sprintf;
use function strlen;
use function unpack;

final readonly class Float64Layout implements Layout
{
    public function encode(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            $values[] = $physical ?? 0.0;
        }

        return [pack('e*', ...$values)];
    }

    public function decode(Buffers $buffers, int $count, array $valid): array
    {
        $buffer = $buffers->next();

        if (strlen($buffer) !== (8 * $count)) {
            throw new InvalidArgumentException(sprintf(
                'Float64 values buffer of %d bytes, expected %d for %d rows',
                strlen($buffer),
                8 * $count,
                $count,
            ));
        }

        if ($count === 0) {
            return [];
        }

        /** @var list<mixed> $physicals */
        $physicals = array_values((array) unpack('e*', $buffer));

        foreach ($valid as $i => $isValid) {
            if (!$isValid) {
                $physicals[$i] = null;
            }
        }

        return $physicals;
    }
}
