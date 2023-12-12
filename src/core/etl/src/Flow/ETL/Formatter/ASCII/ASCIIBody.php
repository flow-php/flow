<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter\ASCII;

use Flow\ETL\Exception\InvalidArgumentException;

final class ASCIIBody
{
    public function __construct(private readonly Headers $headers, private readonly Body $body)
    {
    }

    public function print(int|bool $truncate = 20) : string
    {
        $buffer = '';

        foreach ($this->body->rows() as $row) {
            $buffer .= '|';

            foreach ($this->headers->names() as $name) {
                $header = new ASCIIValue($name);

                try {
                    $value = new ASCIIValue($row->entries()->get($name));
                } catch (InvalidArgumentException $e) {
                    $value = new ASCIIValue('');
                }

                $length = \max($header->length($truncate), $this->body->maximumLength($name, $truncate));

                $buffer .= ' ' . ASCIIValue::mb_str_pad($value->print($truncate), $length, ' ', STR_PAD_LEFT) . ' |';
            }

            $buffer .= PHP_EOL;
        }

        $buffer .= '+';

        foreach ($this->headers->names() as $name) {
            $headerName = new ASCIIValue($name);

            $length = \max($headerName->length($truncate), $this->body->maximumLength($name, $truncate));

            $buffer .= '-' . \str_repeat('-', $length) . '-+';
        }

        if (\count($this->body->partitions())) {
            $buffer .= PHP_EOL;
            $buffer .= 'Partitions:';

            foreach ($this->body->partitions() as $partition) {
                $buffer .= PHP_EOL . ' - ' . $partition->name . '=' . $partition->value;
            }
        }

        return $buffer;
    }
}
