<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

final class VmStackContext
{
    // leftover stack values decide whether the engine fault shows: a never-set opline reads these ints as one
    public static function fillWithInts(): int
    {
        $a0 = 2;
        $a1 = 2;
        $a2 = 2;
        $a3 = 2;
        $a4 = 2;
        $a5 = 2;
        $a6 = 2;
        $a7 = 2;
        $a8 = 2;
        $a9 = 2;
        $b0 = 2;
        $b1 = 2;
        $b2 = 2;
        $b3 = 2;
        $b4 = 2;
        $b5 = 2;
        $b6 = 2;
        $b7 = 2;
        $b8 = 2;
        $b9 = 2;
        $c0 = 2;
        $c1 = 2;
        $c2 = 2;
        $c3 = 2;
        $c4 = 2;
        $c5 = 2;
        $c6 = 2;
        $c7 = 2;
        $c8 = 2;
        $c9 = 2;

        return (
            $a0
            + $a1
            + $a2
            + $a3
            + $a4
            + $a5
            + $a6
            + $a7
            + $a8
            + $a9
            + $b0
            + $b1
            + $b2
            + $b3
            + $b4
            + $b5
            + $b6
            + $b7
            + $b8
            + $b9
            + $c0
            + $c1
            + $c2
            + $c3
            + $c4
            + $c5
            + $c6
            + $c7
            + $c8
            + $c9
        );
    }
}
