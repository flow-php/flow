<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Double;

use Flow\Azure\SDK\BlobService\BlockBlob\BlockList;
use Flow\Azure\SDK\BlobService\CopyBlob\CopyBlobOptions;
use Flow\Azure\SDK\BlobService\CreateContainer\CreateContainerOptions;
use Flow\Azure\SDK\BlobService\DeleteBlob\DeleteBlobOptions;
use Flow\Azure\SDK\BlobService\DeleteContainer\DeleteContainerOptions;
use Flow\Azure\SDK\BlobService\GetBlob\{BlobContent, GetBlobOptions};
use Flow\Azure\SDK\BlobService\GetBlobProperties\{BlobProperties, GetBlobPropertiesOptions};
use Flow\Azure\SDK\BlobService\GetBlockBlobBlockList\GetBlockBlobBlockListOptions;
use Flow\Azure\SDK\BlobService\GetContainerProperties\{ContainerProperties, GetContainerPropertiesOptions};
use Flow\Azure\SDK\BlobService\ListBlobs\ListBlobOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlob\PutBlockBlobOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlock\PutBlockBlobBlockOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\{PutBlockBlobBlockListOptions, SimpleXMLSerializer};
use Flow\Azure\SDK\{BlobServiceInterface, Serializer};

final class RecordingBlobService implements BlobServiceInterface
{
    public int $copyBlobCount = 0;

    public int $deleteBlobCount = 0;

    public int $getBlobPropertiesCount = 0;

    public int $listBlobsCount = 0;

    public function __construct(private readonly BlobServiceInterface $delegate)
    {
    }

    public function copyBlob(string $fromBlob, string $toBlob, CopyBlobOptions $options = new CopyBlobOptions()) : void
    {
        $this->copyBlobCount++;
        $this->delegate->copyBlob($fromBlob, $toBlob, $options);
    }

    public function deleteBlob(string $blob, DeleteBlobOptions $options = new DeleteBlobOptions()) : void
    {
        $this->deleteBlobCount++;
        $this->delegate->deleteBlob($blob, $options);
    }

    public function deleteContainer(DeleteContainerOptions $options = new DeleteContainerOptions()) : void
    {
        $this->delegate->deleteContainer($options);
    }

    public function getBlob(string $blob, GetBlobOptions $options = new GetBlobOptions()) : BlobContent
    {
        return $this->delegate->getBlob($blob, $options);
    }

    public function getBlobProperties(string $blob, GetBlobPropertiesOptions $options = new GetBlobPropertiesOptions()) : ?BlobProperties
    {
        $this->getBlobPropertiesCount++;

        return $this->delegate->getBlobProperties($blob, $options);
    }

    public function getBlockBlobBlockList(string $blob, GetBlockBlobBlockListOptions $options = new GetBlockBlobBlockListOptions()) : BlockList
    {
        return $this->delegate->getBlockBlobBlockList($blob, $options);
    }

    public function getContainerProperties(GetContainerPropertiesOptions $options = new GetContainerPropertiesOptions()) : ?ContainerProperties
    {
        return $this->delegate->getContainerProperties($options);
    }

    public function listBlobs(ListBlobOptions $options = new ListBlobOptions()) : \Generator
    {
        $this->listBlobsCount++;

        yield from $this->delegate->listBlobs($options);
    }

    public function putBlockBlob(string $path, $content = null, ?int $size = null, PutBlockBlobOptions $options = new PutBlockBlobOptions()) : void
    {
        $this->delegate->putBlockBlob($path, $content, $size, $options);
    }

    public function putBlockBlobBlock(string $path, string $blockId, $content, int $size, PutBlockBlobBlockOptions $options = new PutBlockBlobBlockOptions()) : void
    {
        $this->delegate->putBlockBlobBlock($path, $blockId, $content, $size, $options);
    }

    public function putBlockBlobBlockList(string $path, BlockList $blockList, PutBlockBlobBlockListOptions $options = new PutBlockBlobBlockListOptions(), Serializer $serializer = new SimpleXMLSerializer()) : void
    {
        $this->delegate->putBlockBlobBlockList($path, $blockList, $options, $serializer);
    }

    public function putContainer(CreateContainerOptions $options = new CreateContainerOptions()) : void
    {
        $this->delegate->putContainer($options);
    }

    public function resetCounters() : void
    {
        $this->copyBlobCount = 0;
        $this->deleteBlobCount = 0;
        $this->getBlobPropertiesCount = 0;
        $this->listBlobsCount = 0;
    }
}
