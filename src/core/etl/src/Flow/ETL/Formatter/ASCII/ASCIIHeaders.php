<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter\ASCII;

final class ASCIIHeaders
{
    public function __construct(private readonly Headers $headers, private readonly Body $body)
    {
    }

    public function print(int|bool $truncate = 20) : string
    {
        $buffer = '+';

        foreach ($this->headers->names() as $name) {
            $headerName = new ASCIIValue($name);

            $length = \max($headerName->length($truncate), $this->body->maximumLength($name, $truncate));

            $buffer .= \str_repeat('-', $length) . '+';
        }

        $topLine = $buffer;

        $buffer .= PHP_EOL;
        $buffer .= '|';

        foreach ($this->headers->names() as $name) {
            $headerName = new ASCIIValue($name);

            $length = \max($headerName->length($truncate), $this->body->maximumLength($name, $truncate));

            $buffer .= ASCIIValue::mb_str_pad($headerName->print($truncate), $length, ' ', STR_PAD_LEFT) . '|';
        }

        $buffer .= PHP_EOL;

        $buffer .= $topLine . PHP_EOL;

        return $buffer;
    }
}
