<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

use Flow\ETL\Exception\InvalidArgumentException;

use function array_values;
use function pack;
use function sprintf;
use function strlen;
use function unpack;

final readonly class Int32Layout implements Layout
{
    public function encode(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            $values[] = $physical ?? 0;
        }

        return [pack('V*', ...$values)];
    }

    public function decode(Buffers $buffers, int $count, array $valid): array
    {
        $buffer = $buffers->next();

        if (strlen($buffer) !== (4 * $count)) {
            throw new InvalidArgumentException(sprintf(
                'Int32 values buffer of %d bytes, expected %d for %d rows',
                strlen($buffer),
                4 * $count,
                $count,
            ));
        }

        if ($count === 0) {
            return [];
        }

        $physicals = [];

        foreach (array_values((array) unpack('V*', $buffer)) as $i => $value) {
            if (!$valid[$i]) {
                $physicals[] = null;

                continue;
            }

            $unsigned = (int) $value;
            $physicals[] = $unsigned >= 2_147_483_648 ? $unsigned - 4_294_967_296 : $unsigned;
        }

        return $physicals;
    }
}
