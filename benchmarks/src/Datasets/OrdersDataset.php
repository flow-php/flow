<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\Excel\DSL\to_excel;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\Adapter\JSON\to_json_lines;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\Adapter\XML\to_xml;
use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\to_floe;

final readonly class OrdersDataset
{
    public function __construct(
        private int $rows,
    ) {}

    public function csv(): string
    {
        $fixture = new FixturePath('orders', $this->rows, FixtureFormat::csv);

        if ($fixture->exists()) {
            return $fixture->path();
        }

        $fixture->prune();

        data_frame()->read(from_parquet($this->parquet()))->write(to_csv($fixture->path()))->run();

        return $fixture->path();
    }

    public function excel(): string
    {
        $fixture = new FixturePath('orders', $this->rows, FixtureFormat::excel);

        if ($fixture->exists()) {
            return $fixture->path();
        }

        $fixture->prune();

        data_frame()->read(from_parquet($this->parquet()))->write(to_excel($fixture->path()))->run();

        return $fixture->path();
    }

    public function floe(): string
    {
        $fixture = new FixturePath('orders', $this->rows, FixtureFormat::floe);

        if ($fixture->exists()) {
            return $fixture->path();
        }

        $fixture->prune();

        data_frame()->read(from_parquet($this->parquet()))->write(to_floe($fixture->path()))->run();

        return $fixture->path();
    }

    public function json(): string
    {
        $fixture = new FixturePath('orders', $this->rows, FixtureFormat::json);

        if ($fixture->exists()) {
            return $fixture->path();
        }

        $fixture->prune();

        data_frame()->read(from_parquet($this->parquet()))->write(to_json($fixture->path()))->run();

        return $fixture->path();
    }

    public function jsonLines(): string
    {
        $fixture = new FixturePath('orders', $this->rows, FixtureFormat::json_lines);

        if ($fixture->exists()) {
            return $fixture->path();
        }

        $fixture->prune();

        data_frame()->read(from_parquet($this->parquet()))->write(to_json_lines($fixture->path()))->run();

        return $fixture->path();
    }

    /**
     * The root fixture every other format derives from, and fingerprinted like all of them: the
     * generator and the parquet writer are inside the chain FixtureFormat::CHAIN digests.
     */
    public function parquet(): string
    {
        $fixture = new FixturePath('orders', $this->rows, FixtureFormat::parquet);

        if ($fixture->exists()) {
            return $fixture->path();
        }

        $fixture->prune();

        data_frame()->read(new FakeRandomOrdersExtractor($this->rows))->write(to_parquet($fixture->path()))->run();

        return $fixture->path();
    }

    public function xml(): string
    {
        $fixture = new FixturePath('orders', $this->rows, FixtureFormat::xml);

        if ($fixture->exists()) {
            return $fixture->path();
        }

        $fixture->prune();

        data_frame()->read(from_parquet($this->parquet()))->write(to_xml($fixture->path()))->run();

        return $fixture->path();
    }
}
