<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Rows;
use Generator;

/**
 * Records every bucket id each operation was asked for, so a test can assert which storage a run was
 * routed to. BucketsStorage::get() is silent for an unknown id in every implementation, so a routing
 * miss reads zero rows rather than throwing - recording the calls is the only way to see it.
 */
final class RecordingBucketsStorage implements BucketsStorage
{
    /**
     * @var list<string>
     */
    public array $appended = [];

    /**
     * @var list<string>
     */
    public array $read = [];

    public function __construct(
        private readonly BucketsStorage $inner,
    ) {}

    public function append(string $bucketId, Rows $rows): void
    {
        $this->appended[] = $bucketId;
        $this->inner->append($bucketId, $rows);
    }

    public function get(string $bucketId): Generator
    {
        $this->read[] = $bucketId;

        yield from $this->inner->get($bucketId);
    }

    public function remove(string $bucketId): void
    {
        $this->inner->remove($bucketId);
    }

    public function set(string $bucketId, Rows $rows): void
    {
        $this->appended[] = $bucketId;
        $this->inner->set($bucketId, $rows);
    }
}
