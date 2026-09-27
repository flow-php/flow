<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use Flow\ETL\Column\Php\JsonFactory;
use Flow\Types\Value\Json;

use function substr;
use function unpack;

final class JsonDecoder implements ValueDecoder
{
    public function __construct(
        private readonly JsonFactory $factory = new JsonFactory(),
    ) {}

    public function decode(string $data, int &$position): Json
    {
        $length = unpack('V', $data, $position)[1];
        $json = substr($data, $position + 4, $length);
        $position += 5 + $length;

        return $this->factory->create($json);
    }
}
