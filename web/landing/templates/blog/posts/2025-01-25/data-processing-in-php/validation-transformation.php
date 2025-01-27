<?php

use Flow\ETL\DataFrame;
use Flow\ETL\Transformation;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\type_null;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\type_uuid;
use function Flow\ETL\DSL\when;

class Validation implements Transformation
{
    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame
            ->withEntry('valid', lit(true))
            ->withEntry('valid', when(ref('id')->isType(type_uuid(true)), ref('valid'), lit(false)))
            ->withEntry('valid', when(ref('user_email')->isType(type_string())->and(ref('user_email')->size()->between(1, 256)), ref('valid'), lit(false)))
            ->withEntry('valid', when(ref('country')->isType(type_string())->and(ref('country')->size()->equals(2)), ref('valid'), lit(false)))
            ->withEntry('valid', when(ref('state')->isType(type_string())->and(ref('state')->size()->equals(2)), ref('valid'), lit(false)))
            ->withEntry('valid', when(ref('city')->isType(type_string())->and(ref('city')->size()->between(1, 256)), ref('valid'), lit(false)))
            ->withEntry('valid', when(ref('zip')->isType(type_string())->and(ref('zip')->size()->between(4, 12)), ref('valid'), lit(false)))
            ->withEntry('valid', when(ref('address_1')->isType(type_string())->and(ref('address_1')->size()->between(1, 256)), ref('valid'), lit(false)))
            ->withEntry('valid', when(ref('address_2')->isType(type_null())->or(ref('address_2')->size()->between(1, 256)), ref('valid'), lit(false)))
            ->withEntry('valid', when(ref('address_3')->isType(type_null())->or(ref('address_3')->size()->between(1, 256)), ref('valid'), lit(false)))
        ;
    }
}

df()
    ->read(from_csv(__DIR__ . '/import.csv'))
    ->with(new Validation())
;