<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Flow\Azure\SDK\BlobService\ListBlobs\ListBlobOptions;
use Flow\Azure\SDK\BlobService\ListBlobs\OptionInclude;
use Flow\Azure\SDK\BlobService\ListBlobs\OptionShowOnly;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Local\Permissions;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;

use function array_key_exists;
use function array_map;
use function array_values;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_string;
use function is_array;
use function is_bool;
use function is_int;
use function is_string;

/**
 * @type AzureOptions = array{block_location: string, block_permissions: null|PermissionsShape, block_size: int, file_fast_path: bool, list_blob_include: null|list<string>, list_blob_max_results: null|int, list_blob_show_only: null|string, tmp_dir: PathData}
 * @import-type PathData from Path
 * @import-type PermissionsShape from Permissions
 */
final class Options
{
    private BlockFactory $blockFactory;

    /** @var int<1, max> */
    private int $blockSize = 1024 * 1024 * 4;

    private bool $fileFastPath = true;

    /**
     * @var null|array<OptionInclude>
     */
    private ?array $listBlobInclude = null;

    private ?int $listBlobMaxResults = null;

    private ?OptionShowOnly $listBlobShowOnly = null;

    private Path $tmpDir;

    public function __construct()
    {
        $this->blockFactory = new NativeLocalFileBlocksFactory();
        $this->tmpDir = path('azure-blob://_$azure_flow_tmp$/');
    }

    public function blockFactory(): BlockFactory
    {
        return $this->blockFactory;
    }

    /**
     * @return int<1, max>
     */
    public function blockSize(): int
    {
        return $this->blockSize;
    }

    public function fileFastPath(): bool
    {
        return $this->fileFastPath;
    }

    public function listBlobOptions(): ListBlobOptions
    {
        $listBlobOptions = new ListBlobOptions();

        if ($this->listBlobInclude !== null) {
            $listBlobOptions->withInclude(...$this->listBlobInclude);
        }

        if ($this->listBlobMaxResults !== null) {
            $listBlobOptions->withMaxResults($this->listBlobMaxResults);
        }

        if ($this->listBlobShowOnly !== null) {
            $listBlobOptions->withShowOnly($this->listBlobShowOnly);
        }

        return $listBlobOptions;
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(array $data): self
    {
        if (
            !is_string($data['block_location'] ?? null)
            || !array_key_exists('block_permissions', $data)
            || $data['block_permissions'] !== null && !is_array($data['block_permissions'])
            || !is_int($data['block_size'] ?? null)
            || !is_bool($data['file_fast_path'] ?? null)
            || !is_array($data['tmp_dir'] ?? null)
            || !array_key_exists('list_blob_include', $data)
            || !array_key_exists('list_blob_max_results', $data)
            || !array_key_exists('list_blob_show_only', $data)
        ) {
            throw new InvalidArgumentException(
                'Azure options must hold block_location, block_permissions, block_size, file_fast_path, list_blob_include, list_blob_max_results, list_blob_show_only and tmp_dir',
            );
        }

        $options = (new self())
            ->withBlockFactory(
                new NativeLocalFileBlocksFactory(
                    $data['block_location'],
                    $data['block_permissions'] === null ? null : Permissions::fromArray($data['block_permissions']),
                ),
            )
            ->withBlockSize($data['block_size'])
            ->withFileFastPath($data['file_fast_path'])
            ->withTmpDir(Path::fromArray($data['tmp_dir']));

        if ($data['list_blob_include'] !== null) {
            if (!is_array($data['list_blob_include'])) {
                throw new InvalidArgumentException(
                    'Azure option list_blob_include must be a list of OptionInclude case names or null',
                );
            }

            $include = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($data['list_blob_include'] as $case) {
                $include[] = OptionInclude::from(type_string()->cast($case));
            }

            $options->withListBlobInclude(...$include);
        }

        if ($data['list_blob_max_results'] !== null) {
            if (!is_int($data['list_blob_max_results'])) {
                throw new InvalidArgumentException('Azure option list_blob_max_results must be an integer or null');
            }

            $options->withListBlobMaxResults($data['list_blob_max_results']);
        }

        if ($data['list_blob_show_only'] !== null) {
            $options->withListBlobShowOnly(OptionShowOnly::from(type_string()->cast($data['list_blob_show_only'])));
        }

        return $options;
    }

    /**
     * @return null|AzureOptions null when the block factory is not the native local one
     */
    public function normalize(): ?array
    {
        if (!$this->blockFactory instanceof NativeLocalFileBlocksFactory) {
            return null;
        }

        return [
            'block_location' => $this->blockFactory->blockLocation(),
            'block_permissions' => $this->blockFactory->permissions()?->normalize(),
            'block_size' => $this->blockSize,
            'file_fast_path' => $this->fileFastPath,
            'list_blob_include' => $this->listBlobInclude === null
                ? null
                : array_values(array_map(
                    static fn(OptionInclude $include): string => $include->value,
                    $this->listBlobInclude,
                )),
            'list_blob_max_results' => $this->listBlobMaxResults,
            'list_blob_show_only' => $this->listBlobShowOnly?->value,
            'tmp_dir' => $this->tmpDir->normalize(),
        ];
    }

    public function tmpDir(): Path
    {
        return $this->tmpDir;
    }

    public function withBlockFactory(BlockFactory $blockFactory): self
    {
        $this->blockFactory = $blockFactory;

        return $this;
    }

    public function withBlockSize(int $blockSize): self
    {
        if ($blockSize < 1) {
            throw new InvalidArgumentException('Block size must be greater than 0');
        }

        $this->blockSize = $blockSize;

        return $this;
    }

    /**
     * When enabled (default), list() on a single-file path will first attempt getBlobProperties on the blob.
     * If it exists, list() yields just that single FileStatus and never issues listBlobs.
     * This avoids the container-list permission requirement and an extra round-trip when reading single files.
     * Disable if your workloads typically pass folder/prefix paths to list(), to skip the (failing) properties call.
     */
    public function withFileFastPath(bool $enabled = true): self
    {
        $this->fileFastPath = $enabled;

        return $this;
    }

    public function withListBlobInclude(OptionInclude ...$listBlobInclude): self
    {
        $this->listBlobInclude = $listBlobInclude;

        return $this;
    }

    public function withListBlobMaxResults(int $listBlobMaxResults): self
    {
        $this->listBlobMaxResults = $listBlobMaxResults;

        return $this;
    }

    public function withListBlobShowOnly(OptionShowOnly $listBlobShowOnly): self
    {
        $this->listBlobShowOnly = $listBlobShowOnly;

        return $this;
    }

    public function withTmpDir(Path $tmpDir): self
    {
        $this->tmpDir = $tmpDir;

        return $this;
    }
}
