<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use DateInterval;

use function ord;
use function unpack;

final class IntervalDecoder implements ValueDecoder
{
    public function decode(string $data, int &$position): DateInterval
    {
        $invert = ord($data[$position++]);
        $fields = unpack('Vy/Vm/Vd/Vh/Vi/Vs', $data, $position);
        $position += 24;
        $fraction = unpack('e', $data, $position)[1];
        $position += 8;

        $value = new DateInterval('PT0S');
        // @mago-ignore analysis:invalid-property-write
        $value->y = $fields['y'];
        // @mago-ignore analysis:invalid-property-write
        $value->m = $fields['m'];
        // @mago-ignore analysis:invalid-property-write
        $value->d = $fields['d'];
        // @mago-ignore analysis:invalid-property-write
        $value->h = $fields['h'];
        // @mago-ignore analysis:invalid-property-write
        $value->i = $fields['i'];
        // @mago-ignore analysis:invalid-property-write
        $value->s = $fields['s'];
        // @mago-ignore analysis:invalid-property-write
        $value->f = $fraction;
        // @mago-ignore analysis:invalid-property-write
        $value->invert = $invert;

        return $value;
    }
}
