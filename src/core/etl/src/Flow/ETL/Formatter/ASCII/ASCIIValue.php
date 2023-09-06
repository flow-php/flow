<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter\ASCII;

use Flow\ETL\Row\Entry;

final class ASCIIValue
{
    private ?string $stringValue = null;

    public function __construct(private readonly null|string|int|bool|float|array|Entry $value)
    {
    }

    /**
     * Solution and all credits goes to https://stackoverflow.com/a/58272671.
     *
     * @param string $input
     * @param int $length
     * @param string $padding
     * @param int $padType
     * @param string $encoding
     *
     * @return string
     */
    public static function mb_str_pad(string $input, int $length, string $padding = ' ', int $padType = STR_PAD_RIGHT, string $encoding = 'UTF-8') : string
    {
        $result = $input;

        if (($paddingRequired = $length - \mb_strlen($input, $encoding)) > 0) {
            switch ($padType) {
                case STR_PAD_LEFT:
                    return \mb_substr(\str_repeat($padding, $paddingRequired), 0, $paddingRequired, $encoding) . $input;
                case STR_PAD_RIGHT:
                    return $input . \mb_substr(\str_repeat($padding, $paddingRequired), 0, $paddingRequired, $encoding);
                case STR_PAD_BOTH:
                    $leftPaddingLength = (int) \floor($paddingRequired / 2);
                    $rightPaddingLength = $paddingRequired - $leftPaddingLength;

                    return \mb_substr(\str_repeat($padding, $leftPaddingLength), 0, $leftPaddingLength, $encoding) . $input . \mb_substr(\str_repeat($padding, $rightPaddingLength), 0, $rightPaddingLength, $encoding);
            }
        }

        return $result;
    }

    public function length(int|bool $truncate = 20) : int
    {
        return \mb_strlen($this->print($truncate));
    }

    public function print(int|bool $truncate = 20) : string
    {
        if ($truncate === 0) {
            $truncate = false;
        }

        if ($truncate === false) {
            return $this->stringValue();
        }

        if (\is_int($truncate)) {
            if (\mb_strlen($this->stringValue()) <= $truncate) {
                return $this->stringValue();
            }

            return \mb_substr($this->stringValue(), 0, $truncate);
        }

        // $truncate = true - default 20
        if (\mb_strlen($this->stringValue()) <= 20) {
            return $this->stringValue();
        }

        return \substr($this->stringValue(), 0, 20);
    }

    /**
     * @psalm-suppress MixedReturnStatement
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress PossiblyInvalidCast
     * @psalm-suppress MixedMethodCall
     * @psalm-suppress MixedAssignment
     * @psalm-suppress TypeDoesNotContainType
     */
    private function stringValue() : string
    {
        if ($this->stringValue === null) {
            try {
                $val = $this->value;

                if ($val instanceof Entry) {
                    $this->stringValue = $val->toString();

                    if ($val instanceof Entry\XMLEntry || $val instanceof Entry\XMLNodeEntry) {
                        $this->stringValue = \str_replace("\n", '', $this->stringValue);
                    }

                    return $this->stringValue;
                }

                if ($val === null) {
                    $this->stringValue = 'null';

                    return $this->stringValue;
                }

                /** @psalm-suppress InvalidPropertyAssignmentValue */
                $this->stringValue = match (\gettype($val)) {
                    'string' => $val,
                    'boolean' => ($val) ? 'true' : 'false',
                    'double', 'integer' => (string) $val,
                    'array' => \json_encode($val, JSON_THROW_ON_ERROR),
                };
            } catch (\JsonException $e) {
                $this->stringValue = '{...}';
            }
        }

        return $this->stringValue;
    }
}
