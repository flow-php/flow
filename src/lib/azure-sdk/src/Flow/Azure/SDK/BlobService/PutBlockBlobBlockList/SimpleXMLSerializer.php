<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\PutBlockBlobBlockList;

use Flow\Azure\SDK\BlobService\BlockBlob\BlockList;
use Flow\Azure\SDK\Exception\{Exception, InvalidArgumentException};
use Flow\Azure\SDK\Serializer;

final class SimpleXMLSerializer implements Serializer
{
    public function __construct()
    {
        if (!\class_exists('SimpleXMLElement')) {
            throw new Exception('SimpleXML extension is required to use this normalizer');
        }
    }

    public function serialize(mixed $data) : string
    {
        if (!$data instanceof BlockList) {
            throw new InvalidArgumentException('Data must be an instance of BlockList');
        }

        $xml = new \SimpleXMLElement('<BlockList></BlockList>');

        foreach ($data->all() as $block) {
            $xml->addChild($block->state->value, $block->id);
        }

        $xmlString = $xml->asXML();

        if (\is_bool($xmlString)) {
            throw new Exception('Failed to serialize data');
        }

        return $xmlString;
    }
}
