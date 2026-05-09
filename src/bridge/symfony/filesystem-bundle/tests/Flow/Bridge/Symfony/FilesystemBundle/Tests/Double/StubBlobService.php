<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Double;

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

final class StubBlobService implements BlobServiceInterface
{
    public function copyBlob(string $fromBlob, string $toBlob, CopyBlobOptions $options = new CopyBlobOptions()) : void
    {
    }

    public function deleteBlob(string $blob, DeleteBlobOptions $options = new DeleteBlobOptions()) : void
    {
    }

    public function deleteContainer(DeleteContainerOptions $options = new DeleteContainerOptions()) : void
    {
    }

    public function getBlob(string $blob, GetBlobOptions $options = new GetBlobOptions()) : BlobContent
    {
        throw new \LogicException('not implemented');
    }

    public function getBlobProperties(string $blob, GetBlobPropertiesOptions $options = new GetBlobPropertiesOptions()) : ?BlobProperties
    {
        return null;
    }

    public function getBlockBlobBlockList(string $blob, GetBlockBlobBlockListOptions $options = new GetBlockBlobBlockListOptions()) : BlockList
    {
        throw new \LogicException('not implemented');
    }

    public function getContainerProperties(GetContainerPropertiesOptions $options = new GetContainerPropertiesOptions()) : ?ContainerProperties
    {
        return null;
    }

    public function listBlobs(ListBlobOptions $options = new ListBlobOptions()) : \Generator
    {
        yield from [];
    }

    public function putBlockBlob(string $path, $content = null, ?int $size = null, PutBlockBlobOptions $options = new PutBlockBlobOptions()) : void
    {
    }

    public function putBlockBlobBlock(string $path, string $blockId, $content, int $size, PutBlockBlobBlockOptions $options = new PutBlockBlobBlockOptions()) : void
    {
    }

    public function putBlockBlobBlockList(string $path, BlockList $blockList, PutBlockBlobBlockListOptions $options = new PutBlockBlobBlockListOptions(), Serializer $serializer = new SimpleXMLSerializer()) : void
    {
    }

    public function putContainer(CreateContainerOptions $options = new CreateContainerOptions()) : void
    {
    }
}
