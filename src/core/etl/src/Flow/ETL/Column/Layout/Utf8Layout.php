<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

use Flow\ETL\Exception\InvalidArgumentException;

use function assert;
use function implode;
use function is_string;
use function sprintf;
use function strlen;
use function substr;

final readonly class Utf8Layout implements Layout
{
    public function __construct(
        private Offsets $offsets = new Offsets(),
    ) {}

    public function encode(array $physicals): array
    {
        $offsets = [0];
        $offset = 0;
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            if ($physical !== null) {
                assert(is_string($physical));
                $values[] = $physical;
                $offset += strlen($physical);
            }

            $offsets[] = $offset;
        }

        return [$this->offsets->pack($offsets), implode('', $values)];
    }

    public function decode(Buffers $buffers, int $count, array $valid): array
    {
        $offsets = $this->offsets->unpack($buffers->next(), $count, 'Utf8');
        $data = $buffers->next();

        if ($offsets[$count] !== strlen($data)) {
            throw new InvalidArgumentException(sprintf(
                'Utf8 data buffer of %d bytes, the last offset is %d',
                strlen($data),
                $offsets[$count],
            ));
        }

        $physicals = [];

        for ($i = 0; $i < $count; $i++) {
            $physicals[] = $valid[$i] ? substr($data, $offsets[$i], $offsets[$i + 1] - $offsets[$i]) : null;
        }

        return $physicals;
    }
}
