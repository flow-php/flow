<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger\Logger;

use Psr\Log\AbstractLogger;

final class DumpLogger extends AbstractLogger
{
    /**
     * @param mixed $level
     * @param mixed $message
     * @param array<mixed> $context
     */
    public function log($level, $message, array $context = []) : void
    {
        if (!\is_string($message)) {
            return;
        }

        if (\class_exists('\\Symfony\\Component\\VarDumper\\VarDumper')) {
            \Symfony\Component\VarDumper\VarDumper::dump([$message => $context]);
        } else {
            \var_dump([$message => $context]);
        }
    }
}
