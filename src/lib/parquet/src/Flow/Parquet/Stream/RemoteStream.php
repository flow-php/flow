<?php

declare(strict_types=1);

namespace Flow\Parquet\Stream;

use Flow\Parquet\Stream;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\{GetBlobOptions, GetBlobPropertiesResult};
use MicrosoftAzure\Storage\Common\Models\Range;

final class RemoteStream implements Stream
{
    private ?GetBlobPropertiesResult $properties = null;

    public function __construct(private BlobRestProxy $azureBlobRest, private string $container, private string $blobName)
    {
    }

    public function close() : void
    {
    }

    public function isOpen() : bool
    {
        return true;
    }

    public function read(int $length, int $offset, int $whence) : string
    {
        $offset = ($offset < 0) ? $this->totalSize() + $offset : $offset;

        $options = new GetBlobOptions();
        $options->setRange(new Range($offset, $offset + $length - 1));
        $result = $this->azureBlobRest->getBlob(
            $this->container,
            $this->blobName,
            $options
        );

        return stream_get_contents($result->getContentStream());
    }

    private function totalSize() : int
    {
        if ($this->properties === null) {
            $this->properties = $this->azureBlobRest->getBlobProperties($this->container, $this->blobName);
        }

        return $this->properties->getProperties()->getContentLength();
    }
}
