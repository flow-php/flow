<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Socket\Communication;

use Flow\ETL\Cache;
use Flow\ETL\FlowContext;
use Flow\ETL\Partition\PartitionFilter;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\Serializer\Serializable;
use Flow\Serializer\Serializer;

/**
 * @implements Serializable<array{
 *     type: string,
 *     payload: array{
 *      id?: string,
 *      pipes?: Pipes,
 *      cache?: Cache,
 *      cache_id?: string,
 *      rows?: Rows,
 *      partition_entries?: References,
 *      partition_filter?: PartitionFilter
 *     }
 * }>
 */
final class Message implements Serializable
{
    /**
     * @param string $type
     * @param array{
     *     id?: string,
     *     pipes?: Pipes,
     *     cache?: Cache,
     *     cache_id?: string,
     *     rows?: Rows,
     *     partition_entries?: References,
     *     partition_filter?: PartitionFilter
     * } $payload
     */
    private function __construct(private string $type, private array $payload)
    {
    }

    public static function fetch(string $id) : self
    {
        return new self(
            Protocol::CLIENT_FETCH,
            [
                'id' => $id,
            ]
        );
    }

    public static function identify(string $id) : self
    {
        return new self(
            Protocol::CLIENT_IDENTIFY,
            [
                'id' => $id,
            ]
        );
    }

    public static function process(Rows $rows) : self
    {
        return new self(
            Protocol::SERVER_PROCESS,
            [
                'rows' => $rows,
            ]
        );
    }

    public function __serialize() : array
    {
        return [
            'type' => $this->type,
            'payload' => $this->payload,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->type = $data['type'];
        $this->payload = $data['payload'];
    }

    public static function setup(Pipes $pipes, FlowContext $context, string $cacheId) : self
    {
        return new self(
            Protocol::SERVER_SETUP,
            [
                'pipes' => $pipes,
                'cache' => $context->cache(),
                'cache_id' => $cacheId,
                'partition_entries' => $context->partitionEntries(),
                'partition_filter' => $context->partitionFilter(),
            ]
        );
    }

    /**
     * @return array{
     *     id?: string,
     *     pipes?: Pipes,
     *     cache?: Cache,
     *     cache_id?: string,
     *     rows?: Rows,
     *     partition_entries?: References,
     *     partition_filter?: PartitionFilter
     * }
     */
    public function payload() : array
    {
        return $this->payload;
    }

    public function toString(Serializer $serializer) : string
    {
        return '|' . $serializer->serialize($this) . '|';
    }

    public function type() : string
    {
        return $this->type;
    }
}
