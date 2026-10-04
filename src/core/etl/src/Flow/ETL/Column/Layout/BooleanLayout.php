<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

use Flow\ETL\Exception\InvalidArgumentException;

use function chr;
use function intdiv;
use function ord;
use function sprintf;
use function strlen;

final readonly class BooleanLayout implements Layout
{
    public function encode(array $physicals): array
    {
        $bitmap = '';
        $byte = 0;
        $bit = 0;

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            if ($physical === true) {
                $byte |= 1 << $bit;
            }

            if (++$bit === 8) {
                $bitmap .= chr($byte);
                $byte = 0;
                $bit = 0;
            }
        }

        return [$bit === 0 ? $bitmap : $bitmap . chr($byte)];
    }

    public function decode(Buffers $buffers, int $count, array $valid): array
    {
        $buffer = $buffers->next();

        if (strlen($buffer) < intdiv($count + 7, 8)) {
            throw new InvalidArgumentException(sprintf(
                'Boolean values buffer of %d bytes, expected %d for %d rows',
                strlen($buffer),
                intdiv($count + 7, 8),
                $count,
            ));
        }

        $physicals = [];

        for ($i = 0; $i < $count; $i++) {
            $physicals[] = $valid[$i] ? ((ord($buffer[$i >> 3]) >> ($i & 7)) & 1) === 1 : null;
        }

        return $physicals;
    }
}
