<?php

declare(strict_types=1);

namespace Flow\ParquetViewer\Tests\Context;

use Closure;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Style\SymfonyStyle;

final class StyledOutput
{
    /**
     * @param Closure(SymfonyStyle): void $write
     */
    public static function captured(Closure $write): string
    {
        $output = new BufferedOutput();

        $write(new SymfonyStyle(new ArrayInput([]), $output));

        return $output->fetch();
    }
}
