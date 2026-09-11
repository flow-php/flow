<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Double;

use Google\Service\Sheets;
use Google\Service\Sheets\BatchGetValuesResponse;
use Google\Service\Sheets\ValueRange;
use RuntimeException;

use function array_shift;

/**
 * Google\Service\Resource's constructor is deliberately not called - the spy answers from its own FIFO queues and
 * records what it was asked for.
 */
final class SpySpreadsheetsValuesResource extends Sheets\Resource\SpreadsheetsValues
{
    /**
     * The SDK declares every parameter untyped, so each is narrowed on the way in - what the adapter passes is
     * always a string and an option array, and a test asserting on these should not have to re-narrow them.
     *
     * @var list<array{string, array<array-key, mixed>}>
     */
    public array $batchGetCalls = [];

    /**
     * @var list<array{string, string, array<array-key, mixed>}>
     */
    public array $getCalls = [];

    /**
     * @param list<ValueRange> $ranges FIFO for get()
     * @param list<BatchGetValuesResponse> $batches FIFO for batchGet()
     */
    public function __construct(
        private array $ranges = [],
        private array $batches = [],
    ) {}

    public function batchGet($spreadsheetId, $optParams = []): BatchGetValuesResponse
    {
        $this->batchGetCalls[] = [$spreadsheetId, $optParams];

        return array_shift($this->batches) ?? throw new RuntimeException('No BatchGetValuesResponse queued');
    }

    public function get($spreadsheetId, $range, $optParams = []): ValueRange
    {
        $this->getCalls[] = [$spreadsheetId, $range, $optParams];

        return array_shift($this->ranges) ?? throw new RuntimeException('No ValueRange queued');
    }
}
