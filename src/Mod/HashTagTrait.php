<?php

declare(strict_types=1);

namespace Daison\LaravelHorizonCluster\Mod;

/**
 * @author Ilia Antonov <t34.ddt@list.ru>
 */
trait HashTagTrait
{
    protected function getHashTag(): string
    {
        return config('horizon.hash_tag', '{horizon}');
    }
}