<?php

use Flow\ETL\Runtime\Heartbeat\{Heartbeat, Pulse};
use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{config_builder, df, ref};

final readonly class ExtendLease implements Heartbeat
{
    public function __construct(private Lease $lease)
    {
    }

    public function beat(Pulse $pulse): void
    {
        $this->lease->extend(seconds: 300);
    }
}

df(config_builder()->heartbeat(new ExtendLease($lease), everySeconds: 120))
    ->read(from_csv($path))
    ->sortBy(ref('id'))
    ->write(to_csv($out))
    ->run();
