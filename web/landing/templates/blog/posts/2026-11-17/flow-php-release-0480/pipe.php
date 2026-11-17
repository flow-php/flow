<?php

use function Flow\ETL\DSL\{aggregate, df, filter, from_array, group_by, lit, read, ref, run, sum, to_output, with_column, write};

df() |> read(from_array([['a' => 1, 'b' => 1, 'country' => 'PL'], ['a' => 2, 'b' => 3, 'country' => 'US']]))
     |> with_column('total', ref('a')->plus(ref('b')))
     |> filter(ref('total')->greaterThan(lit(3)))
     |> group_by('country')
     |> aggregate([sum(ref('total'))])
     |> write(to_output())
     |> run();
