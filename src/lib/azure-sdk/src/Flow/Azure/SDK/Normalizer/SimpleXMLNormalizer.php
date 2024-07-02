<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Normalizer;

use Flow\Azure\SDK\Exception\Exception;
use Flow\Azure\SDK\Normalizer;

final class SimpleXMLNormalizer implements Normalizer
{
    public function __construct()
    {
        if (!\class_exists('SimpleXMLElement')) {
            throw new Exception('SimpleXML extension is required to use this normalizer');
        }
    }

    public function toArray(string $data) : array
    {
        return $this->normalize(new \SimpleXMLElement($data));
    }

    private function normalize(\SimpleXMLElement|array $xml) : array
    {
        $normalized = [];

        foreach ((array) $xml as $key => $value) {
            $normalizedValue = ($value instanceof \SimpleXMLElement) || is_array($value) ? $this->normalize($value) : $value;

            $normalized[$key] = \is_array($normalizedValue) && !\count($normalizedValue) ? null : $normalizedValue;
        }

        return $normalized;
    }
}
